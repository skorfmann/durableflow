# frozen_string_literal: true

module DurableFlow
  class Error < StandardError; end

  class MissingStepResultError < Error; end

  class UnresumableWorkflowError < Error; end

  class ChildWorkflowFailedError < Error
    attr_reader :run_id, :workflow_class, :error_class, :error_message

    def initialize(run_id:, workflow_class:, error_class: nil, error_message: nil)
      @run_id = run_id
      @workflow_class = workflow_class
      @error_class = error_class
      @error_message = error_message

      details = [ workflow_class, run_id ].compact.join(" ")
      message = "Child workflow #{details} failed"
      message = "#{message}: #{error_class}" if error_class.present?
      message = "#{message} - #{error_message}" if error_message.present?

      super(message)
    end
  end

  class WaitTimeoutError < Error
    attr_reader :event_name, :step_name

    def initialize(event_name:, step_name:)
      @event_name = event_name
      @step_name = step_name
      super("Timed out waiting for event #{event_name.inspect} in step #{step_name.inspect}")
    end
  end

  class Pause < Exception
    attr_reader :reason, :status

    def initialize(reason:, status:)
      @reason = reason
      @status = status
      super("Paused workflow (#{reason})")
    end
  end

  class Interrupt < ActiveJob::Continuation::Interrupt
    attr_reader :reason, :resume_options, :status

    def initialize(reason:, resume_options:, status:)
      @reason = reason
      @resume_options = resume_options
      @status = status
      super("Interrupted workflow (#{reason})")
    end
  end
end
