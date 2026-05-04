# frozen_string_literal: true

module DurableFlow
  module Live
    Change = Struct.new(
      :type,
      :run_id,
      :workflow_class,
      :record_class,
      :record_id,
      :record,
      :snapshot,
      :occurred_at,
      keyword_init: true,
    ) do
      def self.from_record(record, action:)
        workflow_run = workflow_run_for(record)

        new(
          type: "#{record.model_name.element}.#{action}",
          run_id: workflow_run&.run_id,
          workflow_class: workflow_run&.workflow_class,
          record_class: record.class.name,
          record_id: record.id,
          record: record,
          snapshot: record.live_snapshot,
          occurred_at: Time.current,
        )
      end

      def payload
        snapshot.merge(
          type: type,
          run_id: run_id,
          workflow_class: workflow_class,
          record_class: record_class,
          record_id: record_id,
          occurred_at: occurred_at,
        ).compact
      end

      def self.workflow_run_for(record)
        return record if record.is_a?(WorkflowRun)
        return record.workflow_run if record.respond_to?(:workflow_run)
      end
      private_class_method :workflow_run_for
    end

    module Broadcastable
      extend ActiveSupport::Concern

      included do
        after_commit :broadcast_live_change, on: [ :create, :update ]
      end

      def live_snapshot
        attributes.symbolize_keys
      end

      private
        def broadcast_live_change
          action = previously_new_record? ? "created" : "updated"
          DurableFlow.broadcast_change(Change.from_record(self, action: action))
        end
    end
  end
end
