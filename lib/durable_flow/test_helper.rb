# frozen_string_literal: true

require "active_job/test_helper"
require "active_support/concern"
require "active_support/testing/time_helpers"
require "durable_flow"
require "time"

module DurableFlow
  module TestHelper
    extend ActiveSupport::Concern

    included do
      include ActiveJob::TestHelper
      include ActiveSupport::Testing::TimeHelpers
    end

    def clear_durable_flow!(clear_jobs: true, reset_live: true)
      DurableFlow.reset_live_broadcasters! if reset_live
      send(:clear_enqueued_jobs) if clear_jobs && respond_to?(:clear_enqueued_jobs, true)
      send(:clear_performed_jobs) if clear_jobs && respond_to?(:clear_performed_jobs, true)

      WorkflowWait.delete_all
      WorkflowEvent.delete_all
      WorkflowLog.delete_all
      WorkflowStep.delete_all
      WorkflowRun.delete_all
    end

    def durable_flow_run(run_id)
      WorkflowRun.find_by!(run_id: run_id.to_s)
    end

    def durable_flow_run_for(workflow_class)
      WorkflowRun.where(workflow_class: workflow_class_name(workflow_class)).order(:created_at, :id).last ||
        flunk("Expected a DurableFlow run for #{workflow_class_name(workflow_class)}")
    end

    def durable_flow_timeline_for(workflow_or_run)
      durable_flow_resolve_run(workflow_or_run).timeline
    end

    def durable_flow_step(workflow_or_run, name)
      run = durable_flow_resolve_run(workflow_or_run)
      run.workflow_steps.find_by!(name: name.to_s)
    end

    def perform_durable_flow_jobs(**options, &block)
      raise "Include ActiveJob::TestHelper to perform DurableFlow jobs" unless respond_to?(:perform_enqueued_jobs)

      perform_enqueued_jobs(**options, &block)
    end

    def perform_durable_flow_until_idle(at: Time.current, limit: 100, **options)
      raise "Include ActiveJob::TestHelper to perform DurableFlow jobs" unless respond_to?(:perform_enqueued_jobs)

      performed = 0

      limit.times do
        break unless durable_flow_performable_job_enqueued?(at: at)

        before = respond_to?(:performed_jobs) ? performed_jobs.size : 0
        perform_durable_flow_jobs(**options.merge(at: at))
        performed += performed_jobs.size - before if respond_to?(:performed_jobs)
      end

      if durable_flow_performable_job_enqueued?(at: at)
        raise "DurableFlow jobs did not become idle after #{limit} drain attempts"
      end

      performed
    end

    def notify_workflow_event(name, **payload)
      payload.empty? ? DurableFlow.notify(name) : DurableFlow.notify(name, payload)
    end

    def resume_workflows_for(name, **payload)
      notify_workflow_event(name, **payload)
      perform_durable_flow_jobs(at: Time.current)
    end

    def assert_workflow_completed(workflow_or_run)
      assert_workflow_status(workflow_or_run, "completed")
    end

    def assert_workflow_failed(workflow_or_run, error: nil)
      run = assert_workflow_status(workflow_or_run, "failed")

      if error
        expected_class = error.respond_to?(:name) ? error.name : error.to_s
        assert_equal expected_class, run.last_error&.fetch("class", nil)
      end

      run
    end

    def assert_workflow_sleeping(workflow_or_run, step: nil)
      run = assert_workflow_status(workflow_or_run, "sleeping")
      sleep_step = step ? durable_flow_step(run, step) : run.workflow_steps.find_by!(status: "sleeping")

      assert_equal "sleeping", sleep_step.status
      assert sleep_step.metadata_hash["wake_at"].present?, "Expected sleep step #{sleep_step.name.inspect} to store wake_at metadata"
      sleep_step
    end

    def assert_workflow_waiting_for(workflow_or_run, event_name, match: nil)
      run = assert_workflow_status(workflow_or_run, "waiting")
      wait = run.workflow_waits.find_by!(event_name: event_name.to_s)

      assert_equal "pending", wait.status
      assert_equal match, wait.match_value if match
      wait
    end

    def assert_workflow_waiting_for_workflow(workflow_or_run, run_id, step: nil)
      wait = assert_workflow_waiting_for(
        workflow_or_run,
        DurableFlow::WORKFLOW_COMPLETED_EVENT,
        match: { run_id: run_id.to_s },
      )

      assert_equal step.to_s, wait.workflow_step.name if step
      wait
    end

    def assert_step_succeeded(workflow_or_run, name)
      step = durable_flow_step(workflow_or_run, name)

      assert_equal "succeeded", step.status
      step
    end

    def assert_step_result(workflow_or_run, name, expected)
      step = assert_step_succeeded(workflow_or_run, name)

      assert_equal expected, step.result_value
      step
    end

    def assert_step_attempts(workflow_or_run, name, expected)
      step = durable_flow_step(workflow_or_run, name)

      assert_equal expected, step.attempts
      step
    end

    def assert_workflow_log(workflow_or_run, level: nil, message: nil, data: nil)
      run = durable_flow_resolve_run(workflow_or_run)
      assert_log_in(run.workflow_logs.ordered, level: level, message: message, data: data)
    end

    def assert_step_log(workflow_or_run, step_name, level: nil, message: nil, data: nil)
      step = durable_flow_step(workflow_or_run, step_name)
      assert_log_in(step.workflow_logs.ordered, level: level, message: message, data: data)
    end

    def travel_to_next_workflow_wake(workflow_or_run = nil)
      wake_at = next_workflow_wake_at(workflow_or_run)

      assert wake_at, "Expected a sleeping DurableFlow step with wake_at metadata"
      travel_to wake_at
      wake_at
    end

    def next_workflow_wake_at(workflow_or_run = nil)
      scope = WorkflowStep.where(status: "sleeping")
      scope = scope.where(workflow_run: durable_flow_resolve_run(workflow_or_run)) if workflow_or_run

      scope.filter_map { |step| parse_workflow_wake_at(step.metadata_hash["wake_at"]) }.min
    end

    def capture_durable_flow_changes
      changes = []
      subscriber = DurableFlow.on_change { |change| changes << change }

      yield changes
      changes
    ensure
      DurableFlow.unsubscribe_from_changes(subscriber) if subscriber
    end

    def assert_durable_flow_change(changes, type, **payload)
      change = changes.find do |candidate|
        candidate.type == type && payload.all? { |key, value| durable_flow_change_value(candidate, key) == value }
      end

      assert change, "Expected DurableFlow change #{type.inspect} with #{payload.inspect}, got #{changes.map(&:payload).inspect}"
      change
    end

    private
      def assert_workflow_status(workflow_or_run, status)
        run = durable_flow_resolve_run(workflow_or_run).reload

        assert_equal status, run.status
        run
      end

      def assert_log_in(scope, level:, message:, data:)
        logs = scope.to_a
        log = logs.find do |candidate|
          (level.nil? || candidate.level == level.to_s) &&
            (message.nil? || durable_flow_message_matches?(candidate.message, message)) &&
            (data.nil? || candidate.data_value == data)
        end

        assert log, "Expected workflow log with #{ { level: level, message: message, data: data }.compact.inspect }, got #{logs.map { |entry| [ entry.level, entry.message, entry.data_value ] }.inspect}"
        log
      end

      def durable_flow_resolve_run(workflow_or_run)
        case workflow_or_run
        when WorkflowRun
          workflow_or_run
        when String
          durable_flow_run(workflow_or_run)
        else
          durable_flow_run_for(workflow_or_run)
        end
      end

      def durable_flow_message_matches?(actual, expected)
        expected.is_a?(Regexp) ? expected.match?(actual) : actual == expected.to_s
      end

      def durable_flow_change_value(change, key)
        if change.snapshot.key?(key)
          change.snapshot[key]
        elsif change.payload.key?(key)
          change.payload[key]
        elsif change.respond_to?(key)
          change.public_send(key)
        end
      end

      def parse_workflow_wake_at(value)
        return if value.blank?
        return value if value.is_a?(Time)

        Time.iso8601(value.to_s)
      end

      def durable_flow_performable_job_enqueued?(at:)
        return false unless respond_to?(:enqueued_jobs)

        enqueued_jobs.any? do |payload|
          scheduled_at = payload[:at] || payload["at"]
          scheduled_at.blank? || scheduled_at.to_f <= at.to_f
        end
      end

      def workflow_class_name(workflow_class)
        workflow_class.respond_to?(:name) ? workflow_class.name : workflow_class.to_s
      end
  end
end
