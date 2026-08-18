# frozen_string_literal: true

require "test_helper"

class DurableFlowErrorsTest < DurableFlowTestCase
  test "child workflow failure message includes the error details" do
    error = DurableFlow::ChildWorkflowFailedError.new(
      run_id: "run-1",
      workflow_class: "ChildWorkflow",
      error_class: "RuntimeError",
      error_message: "boom",
    )

    assert_equal "Child workflow ChildWorkflow run-1 failed: RuntimeError - boom", error.message
    assert_equal "run-1", error.run_id
    assert_equal "ChildWorkflow", error.workflow_class
    assert_equal "RuntimeError", error.error_class
    assert_equal "boom", error.error_message
    assert_kind_of DurableFlow::Error, error
  end

  test "child workflow failure message omits blank details" do
    error = DurableFlow::ChildWorkflowFailedError.new(run_id: nil, workflow_class: "ChildWorkflow")

    assert_equal "Child workflow ChildWorkflow failed", error.message
  end

  test "child workflow failure message includes the error class without a message" do
    error = DurableFlow::ChildWorkflowFailedError.new(
      run_id: "run-2",
      workflow_class: "ChildWorkflow",
      error_class: "RuntimeError",
    )

    assert_equal "Child workflow ChildWorkflow run-2 failed: RuntimeError", error.message
  end

  test "wait timeout error describes the event and step" do
    error = DurableFlow::WaitTimeoutError.new(event_name: "approved", step_name: "await_approval")

    assert_equal %(Timed out waiting for event "approved" in step "await_approval"), error.message
    assert_equal "approved", error.event_name
    assert_equal "await_approval", error.step_name
  end

  test "pause carries the reason and status" do
    pause = DurableFlow::Pause.new(reason: :sleeping, status: "sleeping")

    assert_equal "Paused workflow (sleeping)", pause.message
    assert_equal :sleeping, pause.reason
    assert_equal "sleeping", pause.status
    assert_kind_of Exception, pause
    assert_not_kind_of StandardError, pause
  end

  test "interrupt carries resume options" do
    interrupt = DurableFlow::Interrupt.new(
      reason: :waiting,
      resume_options: { wait: 5.minutes },
      status: "waiting",
    )

    assert_equal "Interrupted workflow (waiting)", interrupt.message
    assert_equal :waiting, interrupt.reason
    assert_equal({ wait: 5.minutes }, interrupt.resume_options)
    assert_equal "waiting", interrupt.status
    assert_kind_of ActiveJob::Continuation::Interrupt, interrupt
  end
end
