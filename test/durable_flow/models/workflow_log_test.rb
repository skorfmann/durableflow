# frozen_string_literal: true

require "test_helper"

class DurableFlowWorkflowLogTest < DurableFlowTestCase
  setup do
    @run = create_workflow_run
  end

  test "level must be a known log level" do
    log = DurableFlow::WorkflowLog.new(workflow_run: @run, level: "fatal", message: "boom")

    assert_not_predicate log, :valid?
    assert_includes log.errors[:level], "is not included in the list"
  end

  test "message is required" do
    log = DurableFlow::WorkflowLog.new(workflow_run: @run, level: "info", message: "")

    assert_not_predicate log, :valid?
    assert_includes log.errors[:message], "can't be blank"
  end

  test "levels mirror the workflow logger levels" do
    assert_equal DurableFlow::WorkflowLogger::LEVELS, DurableFlow::WorkflowLog::LEVELS
  end

  test "ordered scope sorts by creation time then id" do
    first = create_workflow_log(workflow_run: @run, message: "first", created_at: 2.minutes.ago)
    second = create_workflow_log(workflow_run: @run, message: "second", created_at: 1.minute.ago)
    third = create_workflow_log(workflow_run: @run, message: "third", created_at: 1.minute.ago)

    assert_equal [ first.id, second.id, third.id ], DurableFlow::WorkflowLog.ordered.pluck(:id)
  end

  test "data_value deserializes the stored data" do
    log = create_workflow_log(workflow_run: @run, data: { user_id: 3, nested: { ok: true } })

    assert_equal({ user_id: 3, nested: { ok: true } }, log.reload.data_value)
  end

  test "workflow_step is optional" do
    log = create_workflow_log(workflow_run: @run)

    assert_nil log.workflow_step
    assert_predicate log, :valid?
  end

  test "live_snapshot exposes log state" do
    step = create_workflow_step(@run)
    log = create_workflow_log(workflow_run: @run, workflow_step: step, level: "warn", message: "careful")
    snapshot = log.live_snapshot

    assert_equal log.id, snapshot[:id]
    assert_equal @run.id, snapshot[:workflow_run_id]
    assert_equal step.id, snapshot[:workflow_step_id]
    assert_equal "warn", snapshot[:level]
    assert_equal "careful", snapshot[:message]
  end
end
