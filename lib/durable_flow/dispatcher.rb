# frozen_string_literal: true

module DurableFlow
  class Dispatcher
    class << self
      def dispatch(event)
        WorkflowWait.pending.where(event_name: event.name).find_each do |wait|
          next unless wait.matches_event?(event)

          wait.update!(status: "matched", workflow_event: event)
          wait.workflow_run.update!(status: "ready")
          enqueue(wait.workflow_run)
        end
      end

      def enqueue(workflow_run)
        return if workflow_run.terminal?
        return if workflow_run.serialized_job.blank?

        job = ActiveJob::Base.deserialize(workflow_run.serialized_job)
        job.scheduled_at = nil
        job.enqueue
      end
    end
  end
end
