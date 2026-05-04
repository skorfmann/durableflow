# frozen_string_literal: true

require "test_helper"
require_relative "../../examples/workflows"

class DurableFlowExamplesTest < DurableFlowTestCase
  setup do
    DurableFlowExamples::State.reset!
  end

  test "batch example supports dynamic string step names" do
    DurableFlowExamples::BatchNotifyWorkflow.perform_later([ 3, 5, 8 ])
    perform_enqueued_jobs

    assert_equal [ 3, 5, 8 ], DurableFlowExamples::State.processed_items

    run = DurableFlow::WorkflowRun.find_by!(workflow_class: DurableFlowExamples::BatchNotifyWorkflow.name)
    assert_equal "completed", run.status
    assert_equal %w[notify-3 notify-5 notify-8], run.workflow_steps.order(:created_at).pluck(:name)
  end

  test "cursor example resumes from active job continuation cursor" do
    DurableFlowExamples::CursorWorkflow.perform_later([ "a", "b", "c", "d" ])

    interrupt_job_during_step DurableFlowExamples::CursorWorkflow, :process_items, cursor: 2 do
      assert_enqueued_jobs 1, only: DurableFlowExamples::CursorWorkflow do
        perform_enqueued_jobs
      end
    end

    assert_equal [ "a", "b" ], DurableFlowExamples::State.processed_items

    perform_enqueued_jobs

    assert_equal [ "a", "b", "c", "d" ], DurableFlowExamples::State.processed_items
  end

  test "welcome example sleeps, waits for event, and finalizes" do
    freeze_time do
      DurableFlowExamples::WelcomeWorkflow.perform_later(user_id: 42, trial_id: "trial-1")
      perform_enqueued_jobs(at: Time.current)

      assert_equal [ [ :welcome_email, "user-42@example.test" ] ], DurableFlowExamples::State.side_effects

      travel 10.minutes
      perform_enqueued_jobs(at: Time.current)

      run = DurableFlow::WorkflowRun.find_by!(workflow_class: DurableFlowExamples::WelcomeWorkflow.name)
      assert_equal "waiting", run.status

      Rails.event.notify(:trial_confirmed, trial_id: "trial-1")
      perform_enqueued_jobs(at: Time.current)

      assert_equal [ [ :onboarded, 42, "trial-1" ] ], DurableFlowExamples::State.events
      assert_equal "completed", run.reload.status
    end
  end

  test "parent workflow waits for child workflow completion event" do
    DurableFlowExamples::ParentWorkflow.perform_later("hello")

    perform_enqueued_jobs(at: Time.current)
    perform_enqueued_jobs(at: Time.current)
    perform_enqueued_jobs(at: Time.current)

    assert_equal [ [ :child, "hello" ] ], DurableFlowExamples::State.side_effects
    assert_equal 1, DurableFlowExamples::State.events.length
    assert_equal :parent_finished, DurableFlowExamples::State.events.first.first

    parent = DurableFlow::WorkflowRun.find_by!(workflow_class: DurableFlowExamples::ParentWorkflow.name)
    child = DurableFlow::WorkflowRun.find_by!(workflow_class: DurableFlowExamples::ChildWorkflow.name)
    assert_equal "completed", parent.status
    assert_equal "completed", child.status
    assert_equal child.run_id, DurableFlowExamples::State.events.first.last
  end
end
