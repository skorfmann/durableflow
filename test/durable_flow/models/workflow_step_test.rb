# frozen_string_literal: true

require "test_helper"

class DurableFlowWorkflowStepTest < DurableFlowTestCase
  setup do
    @run = create_workflow_run
  end

  test "complete! stores the serialized result" do
    step = create_workflow_step(@run)

    freeze_time do
      step.complete!({ user_id: 7, at: Time.current })

      assert_predicate step, :succeeded?
      assert_equal "succeeded", step.status
      assert_equal Time.current, step.completed_at
      assert_equal({ user_id: 7, at: Time.current }, step.result_value)
    end
  end

  test "complete! round trips nested results" do
    step = create_workflow_step(@run)
    step.complete!({ items: [ 1, { name: "a" } ] })

    assert_equal({ items: [ 1, { name: "a" } ] }, step.reload.result_value)
  end

  test "fail! records the error payload" do
    step = create_workflow_step(@run, status: "running")
    error = ArgumentError.new("bad input")
    error.set_backtrace([ "line-1", "line-2" ])

    step.fail!(error)
    step.reload

    assert_equal "failed", step.status
    last_error = step.metadata_hash.fetch("last_error")
    assert_equal "ArgumentError", last_error.fetch("class")
    assert_equal "bad input", last_error.fetch("message")
    assert_equal [ "line-1", "line-2" ], last_error.fetch("backtrace")
  end

  test "fail! keeps existing metadata" do
    step = create_workflow_step(@run, metadata: { "wake_at" => "2024-01-01T00:00:00Z" })

    step.fail!(RuntimeError.new("boom"))

    assert_equal "2024-01-01T00:00:00Z", step.reload.metadata_hash.fetch("wake_at")
  end

  test "fail! truncates the backtrace" do
    error = RuntimeError.new("boom")
    error.set_backtrace(Array.new(25) { |index| "line-#{index}" })

    step = create_workflow_step(@run)
    step.fail!(error)

    assert_equal 10, step.reload.metadata_hash.dig("last_error", "backtrace").size
  end

  test "retry! marks the step retrying and stores retry_at" do
    step = create_workflow_step(@run, status: "running")
    retry_at = Time.utc(2024, 5, 1, 12, 30, 45)

    step.retry!(RuntimeError.new("flaky"), retry_at: retry_at)
    step.reload

    assert_equal "retrying", step.status
    assert_equal "flaky", step.metadata_hash.dig("last_error", "message")
    assert_equal retry_at.iso8601(9), step.metadata_hash.fetch("retry_at")
  end

  test "retry! without retry_at omits the retry timestamp" do
    step = create_workflow_step(@run, status: "running")

    step.retry!(RuntimeError.new("flaky"))

    assert_equal "retrying", step.reload.status
    assert_not step.metadata_hash.key?("retry_at")
  end

  test "metadata_hash defaults to an empty hash" do
    assert_equal({}, create_workflow_step(@run).metadata_hash)
    assert_equal({}, create_workflow_step(@run, name: "blank", metadata: {}).metadata_hash)
  end

  test "logs are nullified when the step is deleted" do
    step = create_workflow_step(@run)
    log = create_workflow_log(workflow_run: @run, workflow_step: step)

    step.destroy!

    assert_nil log.reload.workflow_step_id
  end

  test "live_snapshot exposes step state" do
    step = create_workflow_step(@run, name: "charge", status: "running", attempts: 2)
    snapshot = step.live_snapshot

    assert_equal step.id, snapshot[:id]
    assert_equal @run.id, snapshot[:workflow_run_id]
    assert_equal "charge", snapshot[:name]
    assert_equal "running", snapshot[:status]
    assert_equal 2, snapshot[:attempts]
  end
end
