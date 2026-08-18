# frozen_string_literal: true

require "test_helper"

class DurableFlowWorkflowRunTest < DurableFlowTestCase
  test "active scope excludes terminal runs" do
    enqueued = create_workflow_run(status: "enqueued")
    waiting = create_workflow_run(status: "waiting")
    create_workflow_run(status: "completed")
    create_workflow_run(status: "failed")

    assert_equal [ enqueued.id, waiting.id ].sort, DurableFlow::WorkflowRun.active.pluck(:id).sort
  end

  test "status predicates" do
    assert_predicate create_workflow_run(status: "completed"), :completed?
    assert_predicate create_workflow_run(status: "failed"), :failed?
    assert_predicate create_workflow_run(status: "completed"), :terminal?
    assert_predicate create_workflow_run(status: "failed"), :terminal?
    assert_not_predicate create_workflow_run(status: "running"), :terminal?
  end

  test "acquire_execution_lock! stores owner and expiry" do
    run = create_workflow_run

    freeze_time do
      assert run.acquire_execution_lock!(owner: "worker-1", ttl: 5.minutes)

      assert_equal "worker-1", run.execution_locked_by
      assert_equal Time.current, run.execution_locked_at
      assert_equal 5.minutes.from_now, run.execution_lock_expires_at
      assert_predicate run, :execution_locked?
    end
  end

  test "acquire_execution_lock! is denied while another owner holds a live lock" do
    run = create_workflow_run
    run.acquire_execution_lock!(owner: "worker-1", ttl: 5.minutes)

    assert_not run.acquire_execution_lock!(owner: "worker-2", ttl: 5.minutes)
    assert_equal "worker-1", run.reload.execution_locked_by
  end

  test "acquire_execution_lock! takes over an expired lock" do
    run = create_workflow_run
    run.acquire_execution_lock!(owner: "worker-1", ttl: 1.minute)

    travel 2.minutes do
      assert_not_predicate run, :execution_locked?
      assert run.acquire_execution_lock!(owner: "worker-2", ttl: 1.minute)
      assert_equal "worker-2", run.execution_locked_by
    end
  end

  test "acquire_execution_lock! is denied for terminal runs" do
    run = create_workflow_run(status: "completed")

    assert_not run.acquire_execution_lock!(owner: "worker-1", ttl: 5.minutes)
    assert_nil run.reload.execution_locked_by
  end

  test "release_execution_lock! only clears the lock for the owner" do
    run = create_workflow_run
    run.acquire_execution_lock!(owner: "worker-1", ttl: 5.minutes)

    run.release_execution_lock!(owner: "worker-2")
    assert_equal "worker-1", run.reload.execution_locked_by

    run.release_execution_lock!(owner: "worker-1")
    run.reload

    assert_nil run.execution_locked_by
    assert_nil run.execution_locked_at
    assert_nil run.execution_lock_expires_at
    assert_not_predicate run, :execution_locked?
  end

  test "refresh_execution_lock! extends the expiry for the owner only" do
    run = create_workflow_run

    freeze_time do
      run.acquire_execution_lock!(owner: "worker-1", ttl: 1.minute)

      assert_not run.refresh_execution_lock!(owner: "worker-2", ttl: 10.minutes)
      assert_equal 1.minute.from_now, run.reload.execution_lock_expires_at

      assert run.refresh_execution_lock!(owner: "worker-1", ttl: 10.minutes)
      assert_equal 10.minutes.from_now, run.execution_lock_expires_at
    end
  end

  test "execution_locked? is false without an expiry" do
    run = create_workflow_run(execution_locked_by: "worker-1")

    assert_not_predicate run, :execution_locked?
  end

  test "deleting a run deletes its dependent records" do
    run = create_workflow_run
    step = create_workflow_step(run)
    create_workflow_wait(workflow_run: run, workflow_step: step)
    create_workflow_log(workflow_run: run)

    run.destroy!

    assert_equal 0, DurableFlow::WorkflowStep.count
    assert_equal 0, DurableFlow::WorkflowWait.count
    assert_equal 0, DurableFlow::WorkflowLog.count
  end

  test "timeline returns a timeline for the run" do
    run = create_workflow_run

    assert_kind_of DurableFlow::WorkflowTimeline, run.timeline
    assert_equal run, run.timeline.workflow_run
  end

  test "live_snapshot exposes run state" do
    run = create_workflow_run(status: "running", queue_name: "default", priority: 3)
    snapshot = run.live_snapshot

    assert_equal run.id, snapshot[:id]
    assert_equal run.run_id, snapshot[:run_id]
    assert_equal "FactoryWorkflow", snapshot[:workflow_class]
    assert_equal "running", snapshot[:status]
    assert_equal "default", snapshot[:queue_name]
    assert_equal 3, snapshot[:priority]
    assert_equal false, snapshot[:execution_locked]
  end
end
