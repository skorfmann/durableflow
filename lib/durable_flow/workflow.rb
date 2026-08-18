# frozen_string_literal: true

module DurableFlow
  class Workflow < ActiveJob::Base
    include ActiveJob::Continuable

    before_enqueue :record_enqueued_workflow

    alias_method :continuable_step, :step

    attr_reader :workflow_run

    class << self
      def retry_on(*exceptions, **options, &block)
        return super(*exceptions, **options) unless block

        super(*exceptions, **options) do |job, error|
          begin
            block.call(job, error)
          ensure
            job.send(:fail_workflow_after_unhandled_error!, error) if job.is_a?(DurableFlow::Workflow)
          end
        end
      end

      def discard_on(*exceptions, **options, &block)
        super(*exceptions, **options) do |job, error|
          begin
            block.call(job, error) if block
          ensure
            job.send(:fail_workflow_after_unhandled_error!, error) if job.is_a?(DurableFlow::Workflow)
          end
        end
      end
    end

    def perform_now
      super
    rescue Exception => error
      fail_workflow_after_unhandled_error!(error)
      raise
    end

    def retry_job(options = {})
      if options[:error]
        persist_retrying_workflow!(options[:error])
        failed_workflow_step&.retry!(options[:error], retry_at: retry_at_from(options))
      end

      super
    end

    def checkpoint!
      refresh_execution_lock!
      interrupt!(reason: :stopping) if queue_adapter.respond_to?(:stopping?) && queue_adapter.stopping?
    end

    def step(name = nil, start: nil, isolated: false, &block)
      return StepProxy.new(self) if name.nil?

      durable_step(name, start: start, isolated: isolated, &block)
    end

    def sleep_step(name, duration = nil, until_time: nil)
      durable_step(name) do
        step_record = current_workflow_step
        metadata = step_record.metadata_hash
        wake_at = TimeValues.parse(metadata["wake_at"]) || TimeValues.from(duration, explicit_time: until_time)

        raise ArgumentError, "Provide a duration or until: time for sleep step #{name.inspect}" unless wake_at

        if Time.current < wake_at
          metadata["wake_at"] = TimeValues.format(wake_at)
          step_record.update!(status: "sleeping", metadata: metadata)
          pause_or_interrupt!(reason: :sleeping, status: "sleeping", resume_options: { wait_until: wake_at })
        end

        nil
      end
    end

    def wait_for_event_step(name, event_name:, timeout:, match:, allow_past_events: false, &block)
      durable_step(name) do
        step_record = current_workflow_step
        wait = find_or_initialize_wait(step_record, event_name: event_name, timeout: timeout, match: match)

        if (event = matched_event_for(wait, allow_past_events: allow_past_events))
          wait.update!(status: "matched", workflow_event: event)
          payload = event.payload_value
          block ? block.call(payload) : payload
        elsif wait.timeout_at && Time.current >= wait.timeout_at
          wait.update!(status: "timed_out")
          step_record.update!(status: "failed", metadata: step_record.metadata_hash.merge("timeout_at" => TimeValues.format(wait.timeout_at)))
          raise WaitTimeoutError.new(event_name: event_name.to_s, step_name: name.to_s)
        else
          step_record.update!(
            status: "waiting",
            metadata: step_record.metadata_hash.merge(
              "event_name" => event_name.to_s,
              "timeout_at" => (TimeValues.format(wait.timeout_at) if wait.timeout_at),
            ).compact,
          )

          if wait.timeout_at
            pause_or_interrupt!(reason: :waiting, status: "waiting", resume_options: { wait_until: wait.timeout_at })
          else
            pause_or_interrupt!(reason: :waiting, status: "waiting")
          end
        end
      end
    end

    def wait_for_workflow(name, workflow_or_run_id, timeout: nil)
      StepProxy.new(self).wait_for_workflow(name, workflow_or_run_id, timeout: timeout)
    end

    def child_workflow(name, workflow_class = nil, *args, timeout: nil, on_failure: :raise, **kwargs, &block)
      validate_child_workflow_failure_policy!(on_failure)

      base_name = name.to_s
      child = start_child_workflow_step("#{base_name}_start", workflow_class, *args, **kwargs, &block)

      wait_for_child_workflow("#{base_name}_wait", child, timeout: timeout, on_failure: on_failure)
    end

    def invoke_workflow(name, workflow_class = nil, *args, timeout: nil, on_failure: :raise, **kwargs, &block)
      child_workflow(name, workflow_class, *args, timeout: timeout, on_failure: on_failure, **kwargs, &block)
    end

    def invoke_workflows(name, collection, timeout: nil, concurrency: nil, on_failure: :raise, &block)
      validate_child_workflow_failure_policy!(on_failure)
      raise ArgumentError, "Provide a block that returns workflow requests" unless block

      requests = collection.map do |item|
        request = block.call(item)
        unless request.respond_to?(:workflow_key)
          raise ArgumentError, "invoke_each blocks must return a workflow request with a stable workflow_key"
        end

        request
      end

      child_workflows(name, requests, timeout: timeout, concurrency: concurrency, on_failure: on_failure)
    end

    def child_workflows(name, collection = nil, key: nil, timeout: nil, concurrency: nil, on_failure: :raise, &block)
      validate_child_workflow_failure_policy!(on_failure)

      if collection.nil?
        raise ArgumentError, "Provide a child workflow collection or builder block" unless block

        builder = ChildWorkflowBuilder.new
        block.call(builder)
        collection = builder.requests
        block = nil
        key ||= :workflow_key
      end

      batches = child_workflow_batches(collection, concurrency)

      batches.flat_map do |batch|
        children = batch.map do |item|
          item_key = child_workflow_item_key(item, key)
          child = start_child_workflow_step("#{name}_#{item_key}_start") do
            block ? block.call(item) : start_child_workflow_request(item)
          end

          child.merge("key" => item_key)
        end

        children.map do |child|
          completion = wait_for_child_workflow("#{name}_#{child.fetch("key")}_wait", child, timeout: timeout, on_failure: on_failure)
          completion.to_h.with_indifferent_access.merge(
            "key" => child.fetch("key"),
            "run_id" => child.fetch("run_id"),
            "workflow_class" => child["workflow_class"] || IndifferentAccess.fetch(completion, :workflow_class),
          ).compact
        end
      end
    end

    def each_child_workflow(name, collection, key:, timeout: nil, on_failure: :raise, &block)
      raise ArgumentError, "Provide a block that starts each child workflow" unless block

      child_workflows(name, collection, key: key, timeout: timeout, on_failure: on_failure, &block)
    end

    def log
      @workflow_logger ||= WorkflowLogger.new(self)
    end

    private
      attr_reader :current_workflow_step, :failed_workflow_step

      def continue(&block)
        ensure_workflow_run!
        return if workflow_run.terminal?
        return unless acquire_execution_lock!

        begin
          return if workflow_run.terminal?

          if continuation.started?
            self.resumptions += 1
            instrument :resume, **continuation.instrumentation
          end

          mark_workflow_running!
          result = block.call
          complete_workflow!(result)
        rescue Pause => pause
          persist_interrupted_workflow!(pause.status)
        rescue ActiveJob::Continuation::Interrupt => interrupt
          resume_job(interrupt)
        rescue ActiveJob::Continuation::Error => error
          raise
        rescue StandardError => error
          if resume_errors_after_advancing? && continuation.advanced?
            resume_job(exception: error)
          else
            raise
          end
        ensure
          release_execution_lock!
        end
      end

      def acquire_execution_lock!
        @execution_lock_owner ||= "#{job_id}:#{SecureRandom.uuid}"
        workflow_run.acquire_execution_lock!(owner: @execution_lock_owner, ttl: DurableFlow.execution_lock_ttl)
      end

      def release_execution_lock!
        return unless @execution_lock_owner

        workflow_run.release_execution_lock!(owner: @execution_lock_owner)
      end

      def refresh_execution_lock!
        return unless @execution_lock_owner

        workflow_run.refresh_execution_lock!(owner: @execution_lock_owner, ttl: DurableFlow.execution_lock_ttl)
      end

      def durable_step(name, start: nil, isolated: false, &block)
        block = block_from_method(name) unless block_given?
        normalized_name = normalize_step_name(name)
        loaded = false
        value = nil

        continuable_step(normalized_name, start: start, isolated: isolated) do |continuation_step|
          step_record = start_step_record!(normalized_name)

          if step_record.succeeded?
            value = step_record.result_value
            loaded = true
          else
            begin
              @current_workflow_step = step_record
              value = block.arity == 0 ? block.call : block.call(continuation_step)
              step_record.complete!(value)
              loaded = true
            rescue Pause, ActiveJob::Continuation::Interrupt
              raise
            rescue StandardError => error
              @failed_workflow_step = step_record
              step_record.fail!(error)
              raise
            ensure
              @current_workflow_step = nil
            end
          end
        end

        if loaded
          value
        else
          load_completed_step_result!(normalized_name)
        end
      end

      def block_from_method(name)
        step_method = method(name)

        raise ArgumentError, "Step method '#{name}' must accept 0 or 1 arguments" if step_method.arity > 1

        if step_method.parameters.any? { |type, _| type == :key || type == :keyreq }
          raise ArgumentError, "Step method '#{name}' must not accept keyword arguments"
        end

        step_method.arity == 0 ? -> { step_method.call } : step_method
      end

      def normalize_step_name(name)
        case name
        when Symbol
          name
        when String
          name.to_sym
        else
          raise ActiveJob::Continuation::InvalidStepError, "Step '#{name}' must be a Symbol or String, found '#{name.class}'"
        end
      end

      def start_step_record!(name)
        ensure_workflow_run!
        refresh_execution_lock!

        WorkflowStep.create_or_find_by!(workflow_run: workflow_run, name: name.to_s) do |step_record|
          step_record.status = "pending"
          step_record.metadata = {}
        end.tap do |step_record|
          next if step_record.succeeded?

          step_record.update!(
            status: "running",
            started_at: step_record.started_at || Time.current,
            attempts: step_record.attempts + 1,
          )
        end
      end

      def load_completed_step_result!(name)
        step_record = workflow_run.workflow_steps.find_by(name: name.to_s)
        return step_record.result_value if step_record&.succeeded?

        raise MissingStepResultError, "Missing result for completed step #{name.inspect}"
      end

      def find_or_initialize_wait(step_record, event_name:, timeout:, match:)
        (WorkflowWait.find_by(workflow_run: workflow_run, workflow_step: step_record) ||
          WorkflowWait.create!(
            workflow_run: workflow_run,
            workflow_step: step_record,
            event_name: event_name.to_s,
            status: "pending",
            match: Serializer.dump(match || {}),
            timeout_at: (TimeValues.from(timeout) if timeout),
          )).tap do |wait|
          updates = {}
          updates[:event_name] = event_name.to_s if wait.event_name != event_name.to_s
          updates[:match] = Serializer.dump(match || {}) if wait.match.blank?
          updates[:timeout_at] = TimeValues.from(timeout) if timeout && wait.timeout_at.blank?
          wait.update!(updates) if updates.any?
        end
      end

      def matched_event_for(wait, allow_past_events: false)
        return wait.workflow_event if wait.workflow_event

        scope = WorkflowEvent.named(wait.event_name)
        scope = scope.where("created_at >= ?", wait.created_at) unless allow_past_events

        scope.order(:created_at).detect do |event|
          wait.matches_event?(event)
        end
      end

      def pause_or_interrupt!(reason:, status:, resume_options: nil)
        persist_interrupted_workflow!(status)
        instrument :interrupt, reason: reason, **continuation.instrumentation

        if resume_options
          raise Interrupt.new(reason: reason, status: status, resume_options: resume_options)
        else
          raise Pause.new(reason: reason, status: status)
        end
      end

      def resume_job(exception)
        if exception.is_a?(Interrupt)
          executions_for(exception)
          persist_interrupted_workflow!(exception.status)

          if max_resumptions.nil? || resumptions < max_resumptions
            retry_job(**exception.resume_options)
          else
            error = ActiveJob::Continuation::ResumeLimitError.new("Job was resumed a maximum of #{max_resumptions} times")
            fail_workflow!(error)
            raise error
          end
        else
          persist_interrupted_workflow!("retrying")
          super
        end
      end

      def record_enqueued_workflow
        ensure_workflow_run!
        persist_workflow_run!(
          status: workflow_run.status.presence || "enqueued",
          **enqueue_attributes,
        )
      end

      def persist_workflow_run!(status:, **attributes)
        workflow_run.update!(status: status, serialized_job: serialize, **attributes)
      end

      def enqueue_attributes
        { queue_name: queue_name, priority: priority }
      end

      def ensure_workflow_run!
        @workflow_run ||= WorkflowRun.create_or_find_by!(run_id: job_id) do |run|
          run.job_id = job_id
          run.workflow_class = self.class.name
          run.status = "enqueued"
          run.arguments = Serializer.dump(arguments)
          run.queue_name = queue_name
          run.priority = priority
        end
      end

      def mark_workflow_running!
        persist_workflow_run!(
          status: "running",
          started_at: workflow_run.started_at || Time.current,
          **enqueue_attributes,
        )
      end

      def persist_interrupted_workflow!(status)
        persist_workflow_run!(
          status: status,
          interrupted_at: Time.current,
          **enqueue_attributes,
        )
      end

      def persist_retrying_workflow!(error)
        ensure_workflow_run!

        persist_workflow_run!(
          status: "retrying",
          interrupted_at: Time.current,
          last_error: ErrorPayload.for(error),
          **enqueue_attributes,
        )
      end

      def complete_workflow!(result = nil)
        persist_workflow_run!(
          status: "completed",
          completed_at: Time.current,
          last_error: nil,
        )

        notify_workflow_finished(DurableFlow::WORKFLOW_COMPLETED_EVENT, status: "completed", result: result)
      end

      def fail_workflow_after_unhandled_error!(error)
        return unless workflow_run
        return if workflow_run.terminal?

        failed_workflow_step&.fail!(error)
        fail_workflow!(error)
      end

      def fail_workflow!(error)
        persist_workflow_run!(
          status: "failed",
          failed_at: Time.current,
          last_error: ErrorPayload.for(error),
        )

        notify_workflow_finished(
          DurableFlow::WORKFLOW_FAILED_EVENT,
          status: "failed",
          error_class: error.class.name,
          error_message: error.message,
        )
      end

      def notify_workflow_finished(event_name, status:, **details)
        payload = {
          run_id: workflow_run.run_id,
          job_id: job_id,
          workflow_class: self.class.name,
          **details,
        }

        DurableFlow.notify(event_name, payload)
        DurableFlow.notify(DurableFlow::WORKFLOW_FINISHED_EVENT, payload.merge(status: status))
      end

      def retry_at_from(options)
        return options[:wait_until].to_time if options[:wait_until].respond_to?(:to_time)
        return unless options[:wait]

        Time.current + options[:wait].to_i
      end

      def start_child_workflow_step(name, workflow_class = nil, *args, **kwargs, &block)
        durable_step(name) do
          child = if block
            block.arity.zero? ? block.call : block.call(workflow_class)
          else
            raise ArgumentError, "Provide a workflow class or block for child workflow #{name.inspect}" unless workflow_class

            workflow_class.perform_later(*args, **kwargs)
          end

          child_workflow_start_payload(child, workflow_class)
        end
      end

      def wait_for_child_workflow(name, child, timeout: nil, on_failure: :raise)
        validate_child_workflow_failure_policy!(on_failure)

        child = child.to_h.stringify_keys
        wait_for_event_step(
          name,
          event_name: DurableFlow::WORKFLOW_FINISHED_EVENT,
          timeout: timeout,
          match: { run_id: child.fetch("run_id") },
          allow_past_events: true,
        ) do |completion|
          if IndifferentAccess.fetch(completion, :status) == "failed" && on_failure == :raise
            raise_child_workflow_failed!(completion)
          end

          completion
        end
      end

      def validate_child_workflow_failure_policy!(value)
        return if %i[raise return].include?(value)

        raise ArgumentError, "Child workflow on_failure must be :raise or :return"
      end

      def child_workflow_start_payload(child, workflow_class = nil)
        if child.respond_to?(:to_h)
          payload = child.to_h.with_indifferent_access
          if payload[:run_id].present?
            return {
              "run_id" => payload.fetch(:run_id),
              "workflow_class" => payload[:workflow_class],
            }.compact
          end
        end

        {
          "run_id" => extract_child_workflow_run_id(child),
          "workflow_class" => child_workflow_class_name(child, workflow_class),
        }.compact
      end

      def extract_child_workflow_run_id(child)
        run_id = JobReference.run_id_for(child)

        raise ArgumentError, "Child workflow start must return a job, run id, or object responding to job_id" if run_id.blank?

        run_id
      end

      def child_workflow_item_key(item, key)
        key ||= :workflow_key

        value = if key.respond_to?(:call)
          key.call(item)
        elsif item.respond_to?(:fetch)
          item.fetch(key) { item.fetch(key.to_s) }
        else
          item.public_send(key)
        end

        value.to_s
      end

      def child_workflow_batches(collection, concurrency)
        items = collection.to_a
        return [ items ] unless concurrency

        size = concurrency.to_i
        raise ArgumentError, "Child workflow concurrency must be at least 1" if size < 1

        items.each_slice(size).to_a
      end

      def start_child_workflow_request(request)
        if request.respond_to?(:workflow_class)
          workflow_class = request.workflow_class
          args = request.respond_to?(:workflow_args) ? Array(request.workflow_args) : []
          kwargs = child_workflow_request_kwargs(request)

          return child_workflow_start_payload(workflow_class.perform_later(*args, **kwargs), workflow_class)
        end

        unless request.respond_to?(:perform_later)
          raise ArgumentError, "Child workflow request must respond to perform_later or workflow_class"
        end

        request.perform_later
      end

      def child_workflow_request_kwargs(request)
        kwargs = if request.respond_to?(:workflow_kwargs)
          request.workflow_kwargs
        elsif request.respond_to?(:workflow_arguments)
          request.workflow_arguments
        else
          {}
        end

        kwargs ||= {}
        kwargs.respond_to?(:symbolize_keys) ? kwargs.symbolize_keys : kwargs.to_h
      end

      def child_workflow_class_name(child, workflow_class = nil)
        return workflow_class.name if workflow_class.respond_to?(:name)
        return child.class.name if child.respond_to?(:job_id)

        nil
      end

      def raise_child_workflow_failed!(completion)
        raise ChildWorkflowFailedError.new(
          run_id: IndifferentAccess.fetch(completion, :run_id),
          workflow_class: IndifferentAccess.fetch(completion, :workflow_class),
          error_class: IndifferentAccess.fetch(completion, :error_class),
          error_message: IndifferentAccess.fetch(completion, :error_message),
        )
      end
  end
end
