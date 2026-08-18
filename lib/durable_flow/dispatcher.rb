# frozen_string_literal: true

module DurableFlow
  class Dispatcher
    class << self
      def dispatch(event)
        WorkflowWait.pending.where(event_name: event.name).find_each do |wait|
          dispatch_wait(wait, event)
        end
      end

      def enqueue(workflow_run)
        return if workflow_run.terminal?

        if workflow_run.serialized_job.blank?
          raise UnresumableWorkflowError, "Cannot resume workflow run #{workflow_run.run_id}: no serialized job stored"
        end

        job = ActiveJob::Base.deserialize(workflow_run.serialized_job)
        job.scheduled_at = nil
        job.enqueue
      end

      private
        def dispatch_wait(wait, event)
          return unless wait.matches_event?(event)

          wait.update!(status: "matched", workflow_event: event)
          wait.workflow_run.update!(status: "ready")
          enqueue(wait.workflow_run)
        rescue StandardError => error
          DurableFlow.report_error(
            error,
            context: {
              workflow_wait_id: wait.id,
              workflow_run_id: wait.workflow_run_id,
              event_name: event.name,
            },
          )
        end
    end
  end
end
