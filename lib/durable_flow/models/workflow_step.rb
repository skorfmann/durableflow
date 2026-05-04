# frozen_string_literal: true

module DurableFlow
  class WorkflowStep < ApplicationRecord
    self.table_name = "durable_flow_workflow_steps"

    belongs_to :workflow_run, class_name: "DurableFlow::WorkflowRun"

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

    def metadata_hash
      metadata.presence || {}
    end
  end
end
