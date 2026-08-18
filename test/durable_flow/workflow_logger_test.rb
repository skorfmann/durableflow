# frozen_string_literal: true

require "test_helper"

class DurableFlowWorkflowLoggerTest < DurableFlowTestCase
  class LoggingWorkflow < DurableFlow::Workflow
    cattr_accessor :logged, default: ->(_log) {}

    def perform
      self.class.logged.call(log)
      nil
    end
  end

  class StepLoggingWorkflow < DurableFlow::Workflow
    def perform
      step(:work) do
        log.info("inside step")
        nil
      end
    end
  end

  test "each level writes a log for the run" do
    run = perform_logging_workflow do |log|
      log.debug("debug message")
      log.info("info message")
      log.warn("warn message")
      log.error("error message")
    end

    assert_equal DurableFlow::WorkflowLogger::LEVELS, run.workflow_logs.ordered.map(&:level)
    assert_equal [ "debug message", "info message", "warn message", "error message" ],
      run.workflow_logs.ordered.map(&:message)
    assert_equal [ nil ], run.workflow_logs.map(&:workflow_step_id).uniq
  end

  test "write merges positional data and keyword data" do
    run = perform_logging_workflow do |log|
      log.info("with data", { user_id: 1 }, token: "abc")
    end

    assert_equal({ user_id: 1, token: "abc" }, run.workflow_logs.sole.data_value)
  end

  test "write stores an empty hash without data" do
    run = perform_logging_workflow { |log| log.info("bare") }

    assert_equal({}, run.workflow_logs.sole.data_value)
  end

  test "write coerces the message to a string" do
    run = perform_logging_workflow { |log| log.info(:symbol_message) }

    assert_equal "symbol_message", run.workflow_logs.sole.message
  end

  test "write accepts a symbol level" do
    run = perform_logging_workflow { |log| log.write(:warn, "symbol level") }

    assert_equal "warn", run.workflow_logs.sole.level
  end

  test "write rejects unknown levels" do
    error = assert_raises(ArgumentError) do
      perform_logging_workflow { |log| log.write(:fatal, "nope") }
    end

    assert_equal 'Unknown log level "fatal"', error.message
  end

  test "write rejects non hash data" do
    error = assert_raises(ArgumentError) do
      perform_logging_workflow { |log| log.info("bad", [ 1, 2 ]) }
    end

    assert_equal "Workflow log data must be a Hash", error.message
  end

  test "logs written inside a step are attached to that step" do
    StepLoggingWorkflow.perform_later
    perform_enqueued_jobs

    run = DurableFlow::WorkflowRun.find_by!(workflow_class: StepLoggingWorkflow.name)
    log = run.workflow_logs.sole

    assert_equal "inside step", log.message
    assert_equal "work", log.workflow_step.name
  end

  private
    def perform_logging_workflow(&block)
      LoggingWorkflow.logged = block
      LoggingWorkflow.perform_later
      perform_enqueued_jobs

      DurableFlow::WorkflowRun.find_by!(workflow_class: LoggingWorkflow.name)
    end
end
