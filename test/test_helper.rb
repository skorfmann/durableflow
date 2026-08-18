# frozen_string_literal: true

ENV["RAILS_ENV"] = "test"

require "bundler/setup"
require "minitest/autorun"
require "active_support/test_case"
require "active_support/testing/time_helpers"
require "active_job/test_helper"
require "logger"
require "durable_flow"
require "active_job/continuation/test_helper"

ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: ":memory:")
ActiveRecord.default_timezone = :utc
DurableFlow::Schema.define

ActiveJob::Base.queue_adapter = :test
ActiveJob::Base.logger = Logger.new(IO::NULL)
Rails.event.raise_on_error = true
DurableFlow.unsubscribe_from_rails_events!
DurableFlow.subscribe_to_rails_events!

class DurableFlowTestCase < ActiveSupport::TestCase
  include ActiveJob::TestHelper
  include ActiveJob::Continuation::TestHelper
  include ActiveSupport::Testing::TimeHelpers

  setup do
    DurableFlow.reset_live_broadcasters!
    clear_enqueued_jobs
    clear_performed_jobs
    DurableFlow::WorkflowWait.delete_all
    DurableFlow::WorkflowEvent.delete_all
    DurableFlow::WorkflowLog.delete_all
    DurableFlow::WorkflowStep.delete_all
    DurableFlow::WorkflowRun.delete_all
  end

  teardown do
    travel_back
    DurableFlow.reset_live_broadcasters!
    clear_enqueued_jobs
    clear_performed_jobs
  end

  private
    def create_workflow_run(status: "enqueued", workflow_class: "FactoryWorkflow", **attributes)
      DurableFlow::WorkflowRun.create!(
        run_id: attributes.delete(:run_id) || SecureRandom.uuid,
        job_id: attributes.delete(:job_id) || SecureRandom.uuid,
        workflow_class: workflow_class,
        status: status,
        **attributes
      )
    end

    def create_workflow_step(workflow_run, name: "step", status: "pending", **attributes)
      DurableFlow::WorkflowStep.create!(
        workflow_run: workflow_run,
        name: name.to_s,
        status: status,
        **attributes
      )
    end

    def create_workflow_event(name: "approved", payload: {}, occurred_at: Time.current, **attributes)
      DurableFlow::WorkflowEvent.create!(
        name: name.to_s,
        payload: DurableFlow::Serializer.dump(payload),
        occurred_at: occurred_at,
        **attributes
      )
    end

    def create_workflow_wait(workflow_run:, workflow_step:, event_name: "approved", match: {}, status: "pending", **attributes)
      DurableFlow::WorkflowWait.create!(
        workflow_run: workflow_run,
        workflow_step: workflow_step,
        event_name: event_name.to_s,
        status: status,
        match: DurableFlow::Serializer.dump(match),
        **attributes
      )
    end

    def create_workflow_log(workflow_run:, level: "info", message: "message", data: {}, **attributes)
      DurableFlow::WorkflowLog.create!(
        workflow_run: workflow_run,
        level: level,
        message: message,
        data: DurableFlow::Serializer.dump(data),
        **attributes
      )
    end
end
