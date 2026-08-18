# frozen_string_literal: true

require "test_helper"

class DurableFlowChildWorkflowBuilderTest < DurableFlowTestCase
  class ChildWorkflow < DurableFlow::Workflow
    def perform(*, **)
    end
  end

  test "workflow collects requests in order with stringified keys" do
    builder = DurableFlow::ChildWorkflowBuilder.new
    builder.workflow(ChildWorkflow, 1, key: :first, mode: "fast")
    builder.workflow(ChildWorkflow, key: "second")

    first, second = builder.requests

    assert_equal 2, builder.requests.size
    assert_equal "first", first.workflow_key
    assert_equal ChildWorkflow, first.workflow_class
    assert_equal [ 1 ], first.workflow_args
    assert_equal({ mode: "fast" }, first.workflow_kwargs)
    assert_equal "second", second.workflow_key
    assert_empty second.workflow_args
    assert_empty second.workflow_kwargs
  end

  test "requests start empty" do
    assert_empty DurableFlow::ChildWorkflowBuilder.new.requests
  end

  test "request perform_later enqueues the child workflow with its arguments" do
    request = DurableFlow::ChildWorkflowBuilder::Request.new(
      workflow_key: "child",
      workflow_class: ChildWorkflow,
      workflow_args: [ 1, 2 ],
      workflow_kwargs: { mode: "fast" },
    )

    job = nil
    assert_enqueued_jobs 1, only: ChildWorkflow do
      job = request.perform_later
    end

    assert_equal [ 1, 2, { mode: "fast" } ], job.arguments
  end
end
