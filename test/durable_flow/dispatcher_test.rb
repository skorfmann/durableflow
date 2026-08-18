# frozen_string_literal: true

require "test_helper"

class DurableFlowDispatcherTest < DurableFlowTestCase
  class ApprovalWaitWorkflow < DurableFlow::Workflow
    cattr_accessor :completed_tokens, default: []

    def perform(token)
      step.wait_for_event(:approved, timeout: 1.hour)

      step(:after_event) do
        self.class.completed_tokens << token
      end
    end
  end

  test "enqueue raises when a workflow run has no serialized job" do
    ApprovalWaitWorkflow.perform_later("one")
    perform_enqueued_jobs(at: Time.current)

    run = DurableFlow::WorkflowRun.find_by!(workflow_class: ApprovalWaitWorkflow.name)
    run.update_columns(serialized_job: nil)

    error = assert_raises(DurableFlow::UnresumableWorkflowError) do
      DurableFlow::Dispatcher.enqueue(run)
    end

    assert_includes error.message, run.run_id
  end

  test "a wait that cannot be resumed does not block other waits for the same event" do
    ApprovalWaitWorkflow.completed_tokens = []
    ApprovalWaitWorkflow.perform_later("one")
    ApprovalWaitWorkflow.perform_later("two")
    perform_enqueued_jobs(at: Time.current)

    waits = DurableFlow::WorkflowWait.pending.order(:id).to_a
    assert_equal 2, waits.size

    broken_run = waits.first.workflow_run
    healthy_run = waits.last.workflow_run
    broken_run.update_columns(serialized_job: nil)

    assert_error_reported(DurableFlow::UnresumableWorkflowError) do
      DurableFlow.notify(:approved)
      perform_enqueued_jobs(at: Time.current)
    end

    assert_equal "completed", healthy_run.reload.status
    assert_equal "matched", waits.last.reload.status
    refute_equal "completed", broken_run.reload.status
  end
end
