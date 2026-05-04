# frozen_string_literal: true

module DurableFlow
  class WorkflowLogger
    LEVELS = %w[debug info warn error].freeze

    def initialize(workflow)
      @workflow = workflow
    end

    LEVELS.each do |level|
      define_method(level) do |message, data = nil, **kwargs|
        write(level, message, data, **kwargs)
      end
    end

    def write(level, message, data = nil, **kwargs)
      level = level.to_s
      raise ArgumentError, "Unknown log level #{level.inspect}" unless LEVELS.include?(level)

      workflow.send(:ensure_workflow_run!)

      WorkflowLog.create!(
        workflow_run: workflow.workflow_run,
        workflow_step: workflow.send(:current_workflow_step),
        level: level,
        message: message.to_s,
        data: Serializer.dump(normalize_data(data, kwargs)),
      )
    end

    private
      attr_reader :workflow

      def normalize_data(data, kwargs)
        base = data.nil? ? {} : data
        raise ArgumentError, "Workflow log data must be a Hash" unless base.is_a?(Hash)

        base.merge(kwargs)
      end
  end
end
