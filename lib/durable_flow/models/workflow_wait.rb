# frozen_string_literal: true

module DurableFlow
  class WorkflowWait < ApplicationRecord
    self.table_name = "durable_flow_workflow_waits"

    belongs_to :workflow_run, class_name: "DurableFlow::WorkflowRun"
    belongs_to :workflow_step, class_name: "DurableFlow::WorkflowStep"
    belongs_to :workflow_event, class_name: "DurableFlow::WorkflowEvent", optional: true

    scope :pending, -> { where(status: "pending") }

    def match_value
      Serializer.load(self.match)
    end

    def matches_event?(event)
      event.name == event_name && event.matches_payload?(match_value)
    end
  end
end
