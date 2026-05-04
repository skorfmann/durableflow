# frozen_string_literal: true

module DurableFlow
  class WorkflowEvent < ApplicationRecord
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
        actual_value = if actual.respond_to?(:key?) && actual.key?(key)
          actual[key]
        elsif actual.respond_to?(:key?) && actual.key?(key.to_s)
          actual[key.to_s]
        elsif actual.respond_to?(:key?) && actual.key?(key.to_sym)
          actual[key.to_sym]
        else
          actual[key]
        end

        subset?(expected_value, actual_value)
      end
    end
  end
end
