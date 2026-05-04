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
    clear_enqueued_jobs
    clear_performed_jobs
    DurableFlow::WorkflowWait.delete_all
    DurableFlow::WorkflowEvent.delete_all
    DurableFlow::WorkflowStep.delete_all
    DurableFlow::WorkflowRun.delete_all
  end

  teardown do
    travel_back
    clear_enqueued_jobs
    clear_performed_jobs
  end
end
