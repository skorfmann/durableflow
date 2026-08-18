# frozen_string_literal: true

require "test_helper"

class DurableFlowDispatcherTest < DurableFlowTestCase
  class DispatchedWorkflow < DurableFlow::Workflow
    cattr_accessor :calls, default: 0

    def perform
      step(:work) { self.class.calls += 1 }
    end
  end

  test "dispatch matches pending waits and enqueues their runs" do
    run = create_workflow_run(status: "waiting", serialized_job: serialized_job)
    step = create_workflow_step(run, name: "approved", status: "waiting")
    wait = create_workflow_wait(workflow_run: run, workflow_step: step, event_name: "approved", match: { token: "abc" })
    event = create_workflow_event(name: "approved", payload: { token: "abc" })

    assert_enqueued_jobs 1, only: DispatchedWorkflow do
      DurableFlow::Dispatcher.dispatch(event)
    end

    wait.reload
    assert_equal "matched", wait.status
    assert_equal event.id, wait.workflow_event_id
    assert_equal "ready", run.reload.status
  end

  test "dispatch skips waits whose match does not apply" do
    run = create_workflow_run(status: "waiting", serialized_job: serialized_job)
    step = create_workflow_step(run, name: "approved", status: "waiting")
    wait = create_workflow_wait(workflow_run: run, workflow_step: step, event_name: "approved", match: { token: "abc" })

    assert_no_enqueued_jobs do
      DurableFlow::Dispatcher.dispatch(create_workflow_event(name: "approved", payload: { token: "other" }))
    end

    assert_equal "pending", wait.reload.status
    assert_equal "waiting", run.reload.status
  end

  test "dispatch ignores waits for other events and already matched waits" do
    run = create_workflow_run(status: "waiting", serialized_job: serialized_job)
    other_step = create_workflow_step(run, name: "rejected", status: "waiting")
    matched_step = create_workflow_step(run, name: "approved", status: "waiting")
    create_workflow_wait(workflow_run: run, workflow_step: other_step, event_name: "rejected")
    create_workflow_wait(workflow_run: run, workflow_step: matched_step, event_name: "approved", status: "matched")

    assert_no_enqueued_jobs do
      DurableFlow::Dispatcher.dispatch(create_workflow_event(name: "approved"))
    end
  end

  test "enqueue skips terminal runs" do
    run = create_workflow_run(status: "completed", serialized_job: serialized_job)

    assert_no_enqueued_jobs do
      DurableFlow::Dispatcher.enqueue(run)
    end
  end

  test "enqueue skips runs without a serialized job" do
    run = create_workflow_run(status: "waiting")

    assert_no_enqueued_jobs do
      DurableFlow::Dispatcher.enqueue(run)
    end
  end

  test "enqueue clears the scheduled time of the deserialized job" do
    run = create_workflow_run(status: "waiting", serialized_job: serialized_job(scheduled_at: 1.hour.from_now))

    DurableFlow::Dispatcher.enqueue(run)

    assert_nil enqueued_jobs.sole[:at]
  end

  private
    def serialized_job(scheduled_at: nil)
      job = DispatchedWorkflow.new
      job.scheduled_at = scheduled_at
      job.serialize
    end
end
