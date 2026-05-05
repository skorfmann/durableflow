# frozen_string_literal: true

module DurableFlow
  class Workflow < ActiveJob::Base
    include ActiveJob::Continuable

    before_enqueue :record_enqueued_workflow

    alias_method :continuable_step, :step

    attr_reader :workflow_run

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
        wake_at = durable_flow_parse_time(metadata["wake_at"]) || durable_flow_time_from(duration, explicit_time: until_time)

        raise ArgumentError, "Provide a duration or until: time for sleep step #{name.inspect}" unless wake_at

        if Time.current < wake_at
          metadata["wake_at"] = wake_at.utc.iso8601(9)
          step_record.update!(status: "sleeping", metadata: metadata)
          pause_or_interrupt!(reason: :sleeping, status: "sleeping", resume_options: { wait_until: wake_at })
        end

        nil
      end
    end

    def wait_for_event_step(name, event_name:, timeout:, match:, allow_past_events: false)
      durable_step(name) do
        step_record = current_workflow_step
        wait = find_or_initialize_wait(step_record, event_name: event_name, timeout: timeout, match: match)

        if (event = matched_event_for(wait, allow_past_events: allow_past_events))
          wait.update!(status: "matched", workflow_event: event)
          event.payload_value
        elsif wait.timeout_at && Time.current >= wait.timeout_at
          wait.update!(status: "timed_out")
          step_record.update!(status: "failed", metadata: step_record.metadata_hash.merge("timeout_at" => wait.timeout_at.utc.iso8601(9)))
          raise WaitTimeoutError.new(event_name: event_name.to_s, step_name: name.to_s)
        else
          step_record.update!(
            status: "waiting",
            metadata: step_record.metadata_hash.merge(
              "event_name" => event_name.to_s,
              "timeout_at" => wait.timeout_at&.utc&.iso8601(9),
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

    def child_workflow(name, workflow_class = nil, *args, timeout: nil, **kwargs, &block)
      base_name = name.to_s
      run_id = durable_step("#{base_name}_start") do
        child = if block
          block.arity.zero? ? block.call : block.call(workflow_class)
        else
          raise ArgumentError, "Provide a workflow class or block for child workflow #{name.inspect}" unless workflow_class

          workflow_class.perform_later(*args, **kwargs)
        end

        extract_child_workflow_run_id(child)
      end

      wait_for_workflow("#{base_name}_wait", run_id, timeout: timeout)
    end

    def each_child_workflow(name, collection, key:, timeout: nil, &block)
      raise ArgumentError, "Provide a block that starts each child workflow" unless block

      collection.map do |item|
        item_key = child_workflow_item_key(item, key)
        child_workflow("#{name}_#{item_key}", timeout: timeout) { block.call(item) }
      end
    end

    def log
      @workflow_logger ||= WorkflowLogger.new(self)
    end

    private
      attr_reader :current_workflow_step

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
          block.call
          complete_workflow!
        rescue Pause => pause
          persist_interrupted_workflow!(pause.status)
        rescue ActiveJob::Continuation::Interrupt => interrupt
          resume_job(interrupt)
        rescue ActiveJob::Continuation::Error => error
          fail_workflow!(error)
          raise
        rescue StandardError => error
          if resume_errors_after_advancing? && continuation.advanced?
            resume_job(exception: error)
          else
            fail_workflow!(error)
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
            timeout_at: (durable_flow_time_from(timeout) if timeout),
          )).tap do |wait|
          updates = {}
          updates[:event_name] = event_name.to_s if wait.event_name != event_name.to_s
          updates[:match] = Serializer.dump(match || {}) if wait.match.blank?
          updates[:timeout_at] = durable_flow_time_from(timeout) if timeout && wait.timeout_at.blank?
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
        workflow_run.update!(
          status: workflow_run.status.presence || "enqueued",
          serialized_job: serialize,
          queue_name: queue_name,
          priority: priority,
        )
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
        workflow_run.update!(
          status: "running",
          started_at: workflow_run.started_at || Time.current,
          serialized_job: serialize,
          queue_name: queue_name,
          priority: priority,
        )
      end

      def persist_interrupted_workflow!(status)
        workflow_run.update!(
          status: status,
          interrupted_at: Time.current,
          serialized_job: serialize,
          queue_name: queue_name,
          priority: priority,
        )
      end

      def complete_workflow!
        workflow_run.update!(
          status: "completed",
          completed_at: Time.current,
          serialized_job: serialize,
          last_error: nil,
        )

        DurableFlow.notify(DurableFlow::WORKFLOW_COMPLETED_EVENT, {
          run_id: workflow_run.run_id,
          job_id: job_id,
          workflow_class: self.class.name,
        })
      end

      def fail_workflow!(error)
        workflow_run.update!(
          status: "failed",
          failed_at: Time.current,
          serialized_job: serialize,
          last_error: {
            "class" => error.class.name,
            "message" => error.message,
            "backtrace" => Array(error.backtrace).first(10),
          },
        )

        DurableFlow.notify(DurableFlow::WORKFLOW_FAILED_EVENT, {
          run_id: workflow_run.run_id,
          job_id: job_id,
          workflow_class: self.class.name,
          error_class: error.class.name,
          error_message: error.message,
        })
      end

      def extract_child_workflow_run_id(child)
        run_id = child.respond_to?(:job_id) ? child.job_id : child.to_s

        raise ArgumentError, "Child workflow start must return a job, run id, or object responding to job_id" if run_id.blank?

        run_id
      end

      def child_workflow_item_key(item, key)
        value = if key.respond_to?(:call)
          key.call(item)
        elsif item.respond_to?(:fetch)
          item.fetch(key) { item.fetch(key.to_s) }
        else
          item.public_send(key)
        end

        value.to_s
      end

      def durable_flow_time_from(value, explicit_time: nil)
        return explicit_time.to_time if explicit_time.respond_to?(:to_time)
        return nil if value.nil?
        return value.to_time if value.respond_to?(:to_time)

        Time.current + value
      end

      def durable_flow_parse_time(value)
        return if value.blank?
        return value if value.is_a?(Time)

        Time.iso8601(value.to_s)
      end
  end
end
