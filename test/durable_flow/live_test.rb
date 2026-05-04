# frozen_string_literal: true

require "test_helper"

class DurableFlowLiveTest < DurableFlowTestCase
  class LiveWorkflow < DurableFlow::Workflow
    def perform(token)
      step(:prepare) { { token: token } }

      event = step.wait_for_event(:approved, timeout: 1.hour, match: { token: token })

      step(:finish) { event[:token] }
    end
  end

  class RaisingWorkflow < DurableFlow::Workflow
    cattr_accessor :calls, default: 0

    def perform
      step(:work) do
        self.class.calls += 1
        true
      end
    end
  end

  test "live broadcaster receives committed workflow changes" do
    changes = []
    DurableFlow.live_broadcaster = ->(change) { changes << change }

    freeze_time do
      LiveWorkflow.perform_later("live-1")
      perform_enqueued_jobs(at: Time.current)

      run = DurableFlow::WorkflowRun.find_by!(workflow_class: LiveWorkflow.name)
      wait = run.workflow_waits.first

      assert_live_change changes, "workflow_run.created", run_id: run.run_id, status: "enqueued"
      assert_live_change changes, "workflow_run.updated", run_id: run.run_id, status: "waiting"
      assert_live_change changes, "workflow_step.created", run_id: run.run_id, name: "prepare", status: "pending"
      assert_live_change changes, "workflow_step.updated", run_id: run.run_id, name: "prepare", status: "succeeded"
      assert_live_change changes, "workflow_wait.created", run_id: run.run_id, event_name: "approved", status: "pending"

      Rails.event.notify(:approved, token: "live-1")
      perform_enqueued_jobs(at: Time.current)

      assert_live_change changes, "workflow_event.created", name: "approved"
      assert_live_change changes, "workflow_wait.updated", run_id: run.run_id, event_name: "approved", status: "matched"
      assert_live_change changes, "workflow_step.updated", run_id: run.run_id, name: "finish", status: "succeeded"
      assert_live_change changes, "workflow_run.updated", run_id: run.run_id, status: "completed"

      event_change = changes.find { |change| change.type == "workflow_event.created" && change.snapshot[:name] == "approved" }
      assert_kind_of DurableFlow::Live::Change, event_change
      assert_nil event_change.run_id
      assert_equal "DurableFlow::WorkflowEvent", event_change.record_class
      assert_equal "approved", event_change.payload.fetch(:name)
      assert_equal wait.reload.workflow_event_id, event_change.record_id
    end
  end

  test "live subscribers can be registered and removed" do
    changes = []
    subscriber = DurableFlow.on_change { |change| changes << change }

    RaisingWorkflow.calls = 0
    RaisingWorkflow.perform_later
    perform_enqueued_jobs

    assert changes.any? { |change| change.type == "workflow_run.created" }

    DurableFlow.unsubscribe_from_changes(subscriber)
    changes.clear

    RaisingWorkflow.perform_later
    perform_enqueued_jobs

    assert_empty changes
  end

  test "live broadcaster errors do not fail workflow execution" do
    DurableFlow.live_broadcaster = ->(_change) { raise "broadcast failed" }
    RaisingWorkflow.calls = 0

    RaisingWorkflow.perform_later
    perform_enqueued_jobs

    run = DurableFlow::WorkflowRun.find_by!(workflow_class: RaisingWorkflow.name)
    assert_equal "completed", run.status
    assert_equal 1, RaisingWorkflow.calls
  end

  private
    def assert_live_change(changes, type, **snapshot)
      change = changes.find do |candidate|
        candidate.type == type && snapshot.all? { |key, value| live_change_value(candidate, key) == value }
      end

      assert change, "Expected #{type} with #{snapshot.inspect}, got #{changes.map { |c| [ c.type, c.snapshot ] }.inspect}"
    end

    def live_change_value(change, key)
      if change.snapshot.key?(key)
        change.snapshot[key]
      elsif change.respond_to?(key)
        change.public_send(key)
      end
    end
end
