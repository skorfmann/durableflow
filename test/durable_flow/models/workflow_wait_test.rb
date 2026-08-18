# frozen_string_literal: true

require "test_helper"

class DurableFlowWorkflowWaitTest < DurableFlowTestCase
  setup do
    @run = create_workflow_run
    @step = create_workflow_step(@run)
  end

  test "pending scope only returns pending waits" do
    pending = create_workflow_wait(workflow_run: @run, workflow_step: @step)
    matched_step = create_workflow_step(@run, name: "other")
    create_workflow_wait(workflow_run: @run, workflow_step: matched_step, status: "matched")

    assert_equal [ pending.id ], DurableFlow::WorkflowWait.pending.pluck(:id)
  end

  test "match_value deserializes the stored match" do
    wait = create_workflow_wait(workflow_run: @run, workflow_step: @step, match: { token: "abc" })

    assert_equal({ token: "abc" }, wait.reload.match_value)
  end

  test "matches_event? requires the event name and payload subset to match" do
    wait = create_workflow_wait(workflow_run: @run, workflow_step: @step, event_name: "approved", match: { token: "abc" })

    assert wait.matches_event?(create_workflow_event(name: "approved", payload: { token: "abc", extra: 1 }))
    assert_not wait.matches_event?(create_workflow_event(name: "approved", payload: { token: "other" }))
    assert_not wait.matches_event?(create_workflow_event(name: "rejected", payload: { token: "abc" }))
  end

  test "matches_event? without a match accepts any payload" do
    wait = create_workflow_wait(workflow_run: @run, workflow_step: @step, event_name: "approved")

    assert wait.matches_event?(create_workflow_event(name: "approved", payload: { anything: true }))
  end

  test "workflow_event is optional" do
    wait = create_workflow_wait(workflow_run: @run, workflow_step: @step)

    assert_nil wait.workflow_event
    assert_predicate wait, :valid?
  end

  test "live_snapshot exposes wait state" do
    timeout_at = Time.utc(2024, 7, 8, 9, 10, 11)
    wait = create_workflow_wait(
      workflow_run: @run,
      workflow_step: @step,
      event_name: "approved",
      match: { token: "abc" },
      timeout_at: timeout_at,
    )
    snapshot = wait.live_snapshot

    assert_equal wait.id, snapshot[:id]
    assert_equal @run.id, snapshot[:workflow_run_id]
    assert_equal @step.id, snapshot[:workflow_step_id]
    assert_nil snapshot[:workflow_event_id]
    assert_equal "approved", snapshot[:event_name]
    assert_equal "pending", snapshot[:status]
    assert_equal timeout_at, snapshot[:timeout_at]
  end
end
