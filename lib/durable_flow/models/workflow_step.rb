# frozen_string_literal: true

module DurableFlow
  class WorkflowStep < ApplicationRecord
    include Live::Broadcastable

    self.table_name = "durable_flow_workflow_steps"

    belongs_to :workflow_run, class_name: "DurableFlow::WorkflowRun"
    has_many :workflow_logs, class_name: "DurableFlow::WorkflowLog", dependent: :nullify

    def succeeded?
      status == "succeeded"
    end

    def result_value
      Serializer.load(result)
    end

    def complete!(value)
      update!(
        status: "succeeded",
        result: Serializer.dump(value),
        completed_at: Time.current,
      )
    end

    def fail!(error)
      update!(
        status: "failed",
        metadata: metadata_hash.merge("last_error" => error_payload(error)),
      )
    end

    def retry!(error, retry_at: nil)
      metadata = metadata_hash.merge("last_error" => error_payload(error))
      metadata["retry_at"] = retry_at.utc.iso8601(9) if retry_at

      update!(
        status: "retrying",
        metadata: metadata,
      )
    end

    def metadata_hash
      metadata.presence || {}
    end

    def live_snapshot
      {
        id: id,
        workflow_run_id: workflow_run_id,
        name: name,
        status: status,
        attempts: attempts,
        result: result,
        metadata: metadata,
        started_at: started_at,
        completed_at: completed_at,
        created_at: created_at,
        updated_at: updated_at,
      }
    end

    private
      def error_payload(error)
        {
          "class" => error.class.name,
          "message" => error.message,
          "backtrace" => Array(error.backtrace).first(10),
        }
      end
  end
end
