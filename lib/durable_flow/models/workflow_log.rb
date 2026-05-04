# frozen_string_literal: true

module DurableFlow
  class WorkflowLog < ApplicationRecord
    include Live::Broadcastable

    self.table_name = "durable_flow_workflow_logs"

    belongs_to :workflow_run, class_name: "DurableFlow::WorkflowRun"
    belongs_to :workflow_step, class_name: "DurableFlow::WorkflowStep", optional: true

    LEVELS = WorkflowLogger::LEVELS

    validates :level, inclusion: { in: LEVELS }
    validates :message, presence: true

    scope :ordered, -> { order(:created_at, :id) }

    def data_value
      Serializer.load(data)
    end

    def live_snapshot
      {
        id: id,
        workflow_run_id: workflow_run_id,
        workflow_step_id: workflow_step_id,
        level: level,
        message: message,
        data: data,
        created_at: created_at,
        updated_at: updated_at,
      }
    end
  end
end
