# frozen_string_literal: true

module DurableFlow
  class Error < StandardError; end

  class MissingStepResultError < Error; end

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
