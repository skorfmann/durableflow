# frozen_string_literal: true

module DurableFlow
  class EventSubscriber
    def emit(event)
      return unless DurableFlow.database_ready?
      return if Fiber[:durable_flow_recording_event]

      Fiber[:durable_flow_recording_event] = true
      workflow_event = WorkflowEvent.create!(
        name: event.fetch(:name).to_s,
        payload: Serializer.dump(event[:payload] || {}),
        tags: Serializer.dump(event[:tags] || {}),
        context: Serializer.dump(event[:context] || {}),
        source_location: Serializer.dump(event[:source_location] || {}),
        occurred_at: occurred_at(event[:timestamp]),
      )

      Dispatcher.dispatch(workflow_event)
    ensure
      Fiber[:durable_flow_recording_event] = false
    end

    private
      def occurred_at(timestamp)
        return Time.current unless timestamp

        Time.at(timestamp.to_r / 1_000_000_000).utc
      end
  end
end
