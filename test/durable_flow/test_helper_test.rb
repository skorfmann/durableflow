# frozen_string_literal: true

require "test_helper"
require "durable_flow/test_helper"

class DurableFlowTestHelperTest < DurableFlowTestCase
  include DurableFlow::TestHelper

  class HelperWorkflow < DurableFlow::Workflow
    def perform(token)
      log.info("Workflow started", token: token)

      step(:prepare) do
        log.info("Prepared workflow", token: token)
        { token: token }
      end

      step.sleep(:pause, 10.minutes)

      event = step.wait_for_event(:approved, timeout: 1.hour, match: { token: token })

      step(:finish) do
        log.warn("Finishing workflow", token: event[:token])
        event[:token]
      end
    end
  end

  class ChildWaitWorkflow < DurableFlow::Workflow
    def perform(run_id)
      step.wait_for_workflow(:child_done, run_id, timeout: 1.hour)
    end
  end

  test "helpers assert workflow status steps waits logs sleeps and live changes" do
    freeze_time do
      changes = capture_durable_flow_changes do
        HelperWorkflow.perform_later("helper-1")
        perform_durable_flow_until_idle(at: Time.current)
      end

      run = durable_flow_run_for(HelperWorkflow)

      assert_workflow_sleeping run, step: :pause
      assert_step_result run, :prepare, { token: "helper-1" }
      assert_step_attempts run, :prepare, 1
      assert_workflow_log run, level: :info, message: "Workflow started", data: { token: "helper-1" }
      assert_step_log run, :prepare, level: :info, message: /Prepared/, data: { token: "helper-1" }
      assert_durable_flow_change changes, "workflow_log.created", message: "Workflow started"

      travel_to_next_workflow_wake run
      perform_durable_flow_until_idle(at: Time.current)

      assert_workflow_waiting_for run, :approved, match: { token: "helper-1" }

      resume_workflows_for :approved, token: "helper-1"

      assert_workflow_completed run
      assert_step_result run, :finish, "helper-1"
      assert_step_attempts run, :finish, 1
      assert_step_log run, :finish, level: :warn, message: "Finishing workflow", data: { token: "helper-1" }
      assert_equal %w[prepare pause approved finish], durable_flow_timeline_for(run).step_entries.map(&:name)
    end
  end

  test "clear helper removes durable flow state and test jobs" do
    HelperWorkflow.perform_later("helper-2")

    assert_enqueued_jobs 1
    assert_difference -> { DurableFlow::WorkflowRun.count }, -1 do
      clear_durable_flow!
    end

    assert_enqueued_jobs 0
    assert_empty DurableFlow::WorkflowStep.all
    assert_empty DurableFlow::WorkflowWait.all
    assert_empty DurableFlow::WorkflowEvent.all
    assert_empty DurableFlow::WorkflowLog.all
  end

  test "workflow wait helper can assert child workflow waits" do
    freeze_time do
      ChildWaitWorkflow.perform_later("child-run-1")
      perform_durable_flow_until_idle(at: Time.current)

      run = durable_flow_run_for(ChildWaitWorkflow)
      wait = assert_workflow_waiting_for_workflow(run, "child-run-1", step: :child_done)
      assert_equal DurableFlow::WORKFLOW_COMPLETED_EVENT, wait.event_name
    end
  end
end
