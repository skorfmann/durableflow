# frozen_string_literal: true

module DurableFlow
  class WorkflowEvent < ApplicationRecord
    include Live::Broadcastable

    self.table_name = "durable_flow_workflow_events"

    has_many :workflow_waits, class_name: "DurableFlow::WorkflowWait"

    scope :named, ->(name) { where(name: name.to_s) }

    def payload_value
      Serializer.load(payload)
    end

    def matches_payload?(expected)
      self.class.subset?(expected || {}, payload_value)
    end

    def self.subset?(expected, actual)
      return true if expected.blank?
      return expected == actual unless expected.is_a?(Hash)
      return false unless actual.respond_to?(:[])

      expected.all? do |key, expected_value|
        subset?(expected_value, IndifferentAccess.fetch(actual, key))
      end
    end

    def live_snapshot
      {
        id: id,
        name: name,
        payload: payload,
        tags: tags,
        context: context,
        source_location: source_location,
        occurred_at: occurred_at,
        created_at: created_at,
        updated_at: updated_at,
      }
    end
  end
end
