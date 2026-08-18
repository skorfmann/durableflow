# frozen_string_literal: true

require "test_helper"

class DurableFlowEventSubscriberTest < DurableFlowTestCase
  setup do
    @subscriber = DurableFlow::EventSubscriber.new
  end

  test "emit records the event with deserializable payload" do
    freeze_time do
      @subscriber.emit(
        name: "approved",
        payload: { token: "abc" },
        tags: { graphql: true },
        context: { request_id: "req-1" },
        source_location: { file: "app/models/order.rb" },
        timestamp: nil,
      )

      event = DurableFlow::WorkflowEvent.sole

      assert_equal "approved", event.name
      assert_equal({ token: "abc" }, event.payload_value)
      assert_equal({ graphql: true }, DurableFlow::Serializer.load(event.tags))
      assert_equal({ request_id: "req-1" }, DurableFlow::Serializer.load(event.context))
      assert_equal({ file: "app/models/order.rb" }, DurableFlow::Serializer.load(event.source_location))
      assert_equal Time.current, event.occurred_at
    end
  end

  test "emit defaults missing attributes to empty hashes" do
    @subscriber.emit(name: :approved)
    event = DurableFlow::WorkflowEvent.sole

    assert_equal "approved", event.name
    assert_equal({}, event.payload_value)
    assert_equal({}, DurableFlow::Serializer.load(event.tags))
    assert_equal({}, DurableFlow::Serializer.load(event.context))
  end

  test "emit converts nanosecond timestamps to utc times" do
    occurred_at = Time.utc(2024, 6, 5, 4, 3, 2)

    @subscriber.emit(name: "approved", timestamp: (occurred_at.to_r * 1_000_000_000).to_i)

    assert_equal occurred_at, DurableFlow::WorkflowEvent.sole.occurred_at
  end

  test "emit dispatches to pending waits" do
    run = create_workflow_run(status: "waiting", serialized_job: WaitingWorkflow.new.serialize)
    step = create_workflow_step(run, name: "approved", status: "waiting")
    wait = create_workflow_wait(workflow_run: run, workflow_step: step, event_name: "approved", match: { token: "abc" })

    assert_enqueued_jobs 1, only: WaitingWorkflow do
      @subscriber.emit(name: "approved", payload: { token: "abc" })
    end

    assert_equal "matched", wait.reload.status
    assert_equal DurableFlow::WorkflowEvent.sole.id, wait.workflow_event_id
  end

  test "emit is a no op while another event is being recorded" do
    Fiber[:durable_flow_recording_event] = true

    begin
      @subscriber.emit(name: "approved")

      assert_equal 0, DurableFlow::WorkflowEvent.count
    ensure
      Fiber[:durable_flow_recording_event] = false
    end
  end

  test "emit resets the recording flag" do
    @subscriber.emit(name: "approved")

    assert_not Fiber[:durable_flow_recording_event]
  end

  test "emit resets the recording flag when recording raises" do
    assert_raises(KeyError) { @subscriber.emit(payload: {}) }

    assert_not Fiber[:durable_flow_recording_event]
  end

  class WaitingWorkflow < DurableFlow::Workflow
    def perform
      step.wait_for_event(:approved)
    end
  end
end
