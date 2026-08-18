# frozen_string_literal: true

require "test_helper"

class DurableFlowWorkflowEventTest < DurableFlowTestCase
  test "named scope filters by event name" do
    approved = create_workflow_event(name: "approved")
    create_workflow_event(name: "rejected")

    assert_equal [ approved.id ], DurableFlow::WorkflowEvent.named(:approved).pluck(:id)
  end

  test "payload_value deserializes the stored payload" do
    event = create_workflow_event(payload: { token: "abc", count: 2 })

    assert_equal({ token: "abc", count: 2 }, event.reload.payload_value)
  end

  test "matches_payload? compares a subset of the payload" do
    event = create_workflow_event(payload: { token: "abc", nested: { id: 1, name: "x" } })

    assert event.matches_payload?({})
    assert event.matches_payload?(nil)
    assert event.matches_payload?({ token: "abc" })
    assert event.matches_payload?({ nested: { id: 1 } })
    assert_not event.matches_payload?({ token: "other" })
    assert_not event.matches_payload?({ nested: { id: 2 } })
    assert_not event.matches_payload?({ missing: true })
  end

  test "subset? matches indifferently across symbol and string keys" do
    assert DurableFlow::WorkflowEvent.subset?({ token: "abc" }, { "token" => "abc" })
    assert DurableFlow::WorkflowEvent.subset?({ "token" => "abc" }, { token: "abc" })
    assert DurableFlow::WorkflowEvent.subset?({ a: { b: "c" } }, { "a" => { "b" => "c" } })
  end

  test "subset? treats blank expectations as matching" do
    assert DurableFlow::WorkflowEvent.subset?({}, { token: "abc" })
    assert DurableFlow::WorkflowEvent.subset?(nil, { token: "abc" })
    assert DurableFlow::WorkflowEvent.subset?({}, nil)
  end

  test "subset? compares scalars by equality" do
    assert DurableFlow::WorkflowEvent.subset?("abc", "abc")
    assert_not DurableFlow::WorkflowEvent.subset?("abc", "xyz")
    assert DurableFlow::WorkflowEvent.subset?(1, 1)
  end

  test "subset? is false when the actual value cannot be indexed" do
    assert_not DurableFlow::WorkflowEvent.subset?({ token: "abc" }, nil)
    assert_not DurableFlow::WorkflowEvent.subset?({ token: "abc" }, Object.new)
  end

  test "subset? supports array values" do
    assert DurableFlow::WorkflowEvent.subset?({ 0 => "a" }, [ "a", "b" ])
    assert_not DurableFlow::WorkflowEvent.subset?({ 0 => "z" }, [ "a", "b" ])
  end

  test "live_snapshot exposes event state" do
    occurred_at = Time.utc(2024, 3, 4, 5, 6, 7)
    event = create_workflow_event(name: "approved", payload: { token: "abc" }, occurred_at: occurred_at)
    snapshot = event.live_snapshot

    assert_equal event.id, snapshot[:id]
    assert_equal "approved", snapshot[:name]
    assert_equal occurred_at, snapshot[:occurred_at]
    assert_equal event.payload, snapshot[:payload]
  end

  test "workflow_waits association returns waits pointing at the event" do
    run = create_workflow_run
    step = create_workflow_step(run)
    event = create_workflow_event
    wait = create_workflow_wait(workflow_run: run, workflow_step: step, status: "matched", workflow_event: event)

    assert_equal [ wait.id ], event.workflow_waits.pluck(:id)
  end
end
