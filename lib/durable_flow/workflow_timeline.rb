# frozen_string_literal: true

module DurableFlow
  class WorkflowTimeline
    ITEM_TYPE_ORDER = {
      step: 0,
      wait: 1,
      event: 2,
      log: 3,
    }.freeze

    Item = Struct.new(:type, :record, :timestamp, :step, :step_id, keyword_init: true) do
      def id
        "#{type}-#{record.id}"
      end

      def name
        case type
        when :step
          record.name
        when :wait
          record.event_name
        when :event
          record.name
        when :log
          record.message
        end
      end

      def status
        record.status if record.respond_to?(:status)
      end

      def run_level?
        step_id.nil?
      end
    end

    StepEntry = Struct.new(:step, :logs, :waits, :events, keyword_init: true) do
      def id
        step.id
      end

      def name
        step.name
      end

      def status
        step.status
      end

      def attempts
        step.attempts
      end
    end

    attr_reader :workflow_run

    def initialize(workflow_run)
      @workflow_run = workflow_run
    end

    def step_entries
      @step_entries ||= steps.map do |step|
        waits = waits_for(step)

        StepEntry.new(
          step: step,
          logs: logs_for(step),
          waits: waits,
          events: waits.filter_map(&:workflow_event),
        )
      end
    end

    def step_entry_for(step_or_id)
      step_entries.find { |entry| entry.id == record_id(step_or_id) }
    end

    def steps
      @steps ||= workflow_run.workflow_steps.order(:created_at, :id).to_a
    end

    def waits
      @waits ||= workflow_run.workflow_waits.includes(:workflow_event).order(:created_at, :id).to_a
    end

    def logs
      @logs ||= workflow_run.workflow_logs.includes(:workflow_step).ordered.to_a
    end

    def events
      @events ||= waits.filter_map(&:workflow_event).uniq
    end

    def run_logs
      @run_logs ||= logs.reject(&:workflow_step_id)
    end

    def logs_for(step_or_id)
      logs_by_step_id.fetch(record_id(step_or_id), [])
    end

    def waits_for(step_or_id)
      waits_by_step_id.fetch(record_id(step_or_id), [])
    end

    def items
      @items ||= [
        steps.map { |step| item_for_step(step) },
        waits.map { |wait| item_for_wait(wait) },
        events.map { |event| item_for_event(event) },
        logs.map { |log| item_for_log(log) },
      ].flatten.sort_by { |item| item_sort_key(item) }
    end

    private
      def logs_by_step_id
        @logs_by_step_id ||= logs.select(&:workflow_step_id).group_by(&:workflow_step_id)
      end

      def waits_by_step_id
        @waits_by_step_id ||= waits.group_by(&:workflow_step_id)
      end

      def wait_by_event_id
        @wait_by_event_id ||= waits.select(&:workflow_event_id).index_by(&:workflow_event_id)
      end

      def item_for_step(step)
        Item.new(
          type: :step,
          record: step,
          timestamp: step.started_at || step.created_at,
          step: step,
          step_id: step.id,
        )
      end

      def item_for_wait(wait)
        Item.new(
          type: :wait,
          record: wait,
          timestamp: wait.created_at,
          step: step_by_id[wait.workflow_step_id],
          step_id: wait.workflow_step_id,
        )
      end

      def item_for_event(event)
        wait = wait_by_event_id[event.id]

        Item.new(
          type: :event,
          record: event,
          timestamp: event.occurred_at || event.created_at,
          step: step_by_id[wait&.workflow_step_id],
          step_id: wait&.workflow_step_id,
        )
      end

      def item_for_log(log)
        Item.new(
          type: :log,
          record: log,
          timestamp: log.created_at,
          step: step_by_id[log.workflow_step_id],
          step_id: log.workflow_step_id,
        )
      end

      def step_by_id
        @step_by_id ||= steps.index_by(&:id)
      end

      def item_sort_key(item)
        [
          item.timestamp || Time.at(0),
          ITEM_TYPE_ORDER.fetch(item.type),
          item.record.id || 0,
        ]
      end

      def record_id(record_or_id)
        record_or_id.respond_to?(:id) ? record_or_id.id : record_or_id
      end
  end
end
