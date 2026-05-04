# frozen_string_literal: true

require "test_helper"
require "solid_queue"
require "active_job/concurrency_controls"
require "active_job/queue_adapters/solid_queue_adapter"
require "securerandom"

ActiveJob::Base.include(ActiveJob::ConcurrencyControls) unless ActiveJob::Base.method_defined?(:concurrency_key)

solid_queue_root = Gem.loaded_specs.fetch("solid_queue").full_gem_path
SOLID_QUEUE_TEST_LOADER = Zeitwerk::Loader.new
SOLID_QUEUE_TEST_LOADER.push_dir(File.join(solid_queue_root, "app/models"))
SOLID_QUEUE_TEST_LOADER.push_dir(File.join(solid_queue_root, "app/jobs"))
SOLID_QUEUE_TEST_LOADER.setup
SOLID_QUEUE_TEST_LOADER.eager_load

solid_queue_schema = File.join(
  solid_queue_root,
  "lib/generators/solid_queue/install/templates/db/queue_schema.rb",
)

unless ActiveRecord::Base.connection.data_source_exists?(:solid_queue_jobs)
  begin
    previous_verbose = ActiveRecord::Migration.verbose
    ActiveRecord::Migration.verbose = false
    load solid_queue_schema
  ensure
    ActiveRecord::Migration.verbose = previous_verbose unless previous_verbose.nil?
  end
end

DurableFlow::Schema.define

class DurableFlowSolidQueueIntegrationTest < ActiveSupport::TestCase
  include ActiveSupport::Testing::TimeHelpers

  class SolidWelcomeWorkflow < DurableFlow::Workflow
    cattr_accessor :events, default: []

    def perform(trial_id)
      trial = step(:create_trial) do
        log.info("Created trial", trial_id: trial_id)
        self.class.events << [ :created, trial_id ]
        { id: trial_id }
      end

      step.sleep(:trial_delay, 10.minutes)

      event = step.wait_for_event(:trial_confirmed, timeout: 1.hour, match: { trial_id: trial[:id] })

      step(:finalize) do
        log.info("Finalized trial", trial_id: event[:trial_id])
        self.class.events << [ :finalized, event[:trial_id] ]
        true
      end
    end
  end

  setup do
    @previous_queue_adapter = ActiveJob::Base.queue_adapter
    @previous_solid_queue_logger = SolidQueue.logger
    @previous_use_skip_locked = SolidQueue.use_skip_locked

    ActiveJob::Base.queue_adapter = :solid_queue
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    SolidQueue.logger = Logger.new(IO::NULL)
    SolidQueue.use_skip_locked = false

    clear_solid_queue_tables
    clear_durable_flow_tables
    SolidWelcomeWorkflow.events = []
    @live_changes = []
    DurableFlow.live_broadcaster = ->(change) { @live_changes << change }
  end

  teardown do
    travel_back
    DurableFlow.reset_live_broadcasters!
    clear_solid_queue_tables
    clear_durable_flow_tables

    ActiveJob::Base.queue_adapter = @previous_queue_adapter
    SolidQueue.logger = @previous_solid_queue_logger
    SolidQueue.use_skip_locked = @previous_use_skip_locked
  end

  test "durable workflows resume sleeps and event waits on Solid Queue" do
    freeze_time do
      SolidWelcomeWorkflow.perform_later("solid-trial-1")

      assert_equal 1, SolidQueue::ReadyExecution.count
      assert_equal 0, SolidQueue::ScheduledExecution.count

      assert_equal 1, drain_solid_queue

      run = DurableFlow::WorkflowRun.find_by!(workflow_class: SolidWelcomeWorkflow.name)
      assert_equal "sleeping", run.status
      assert_equal [ [ :created, "solid-trial-1" ] ], SolidWelcomeWorkflow.events

      sleep_resume = SolidQueue::ScheduledExecution.first
      assert sleep_resume, "Expected Solid Queue to hold the delayed sleep resume"
      assert_in_delta 10.minutes.from_now.to_f, sleep_resume.scheduled_at.to_f, 1

      travel 10.minutes
      assert_equal 1, drain_solid_queue

      wait = run.reload.workflow_waits.first
      assert_equal "waiting", run.status
      assert_equal "trial_confirmed", wait.event_name
      assert_equal 1.hour.from_now, wait.timeout_at

      timeout_resume = SolidQueue::ScheduledExecution.first
      assert timeout_resume, "Expected Solid Queue to hold the wait timeout resume"
      assert_in_delta 1.hour.from_now.to_f, timeout_resume.scheduled_at.to_f, 1

      Rails.event.notify(:trial_confirmed, trial_id: "solid-trial-1")

      assert_equal "matched", wait.reload.status
      assert_equal 1, SolidQueue::ReadyExecution.count
      assert_equal 1, drain_solid_queue

      assert_equal "completed", run.reload.status
      assert_equal [
        [ :created, "solid-trial-1" ],
        [ :finalized, "solid-trial-1" ],
      ], SolidWelcomeWorkflow.events

      travel 1.hour
      assert_equal 1, drain_solid_queue

      assert_equal "completed", run.reload.status
      assert_equal [
        [ :created, "solid-trial-1" ],
        [ :finalized, "solid-trial-1" ],
      ], SolidWelcomeWorkflow.events

      assert_live_change "workflow_run.updated", run_id: run.run_id, status: "sleeping"
      assert_live_change "workflow_run.updated", run_id: run.run_id, status: "waiting"
      assert_live_change "workflow_run.updated", run_id: run.run_id, status: "completed"
      assert_live_change "workflow_step.updated", run_id: run.run_id, name: "trial_delay", status: "sleeping"
      assert_live_change "workflow_wait.created", run_id: run.run_id, event_name: "trial_confirmed", status: "pending"
      assert_live_change "workflow_wait.updated", run_id: run.run_id, event_name: "trial_confirmed", status: "matched"
      assert_live_change "workflow_event.created", name: "trial_confirmed"
      assert_live_change "workflow_log.created", run_id: run.run_id, level: "info", message: "Created trial"
      assert_live_change "workflow_log.created", run_id: run.run_id, level: "info", message: "Finalized trial"
    end
  end

  private
    def assert_live_change(type, **snapshot)
      change = @live_changes.find do |candidate|
        candidate.type == type && snapshot.all? { |key, value| live_change_value(candidate, key) == value }
      end

      assert change, "Expected #{type} with #{snapshot.inspect}"
    end

    def live_change_value(change, key)
      if change.snapshot.key?(key)
        change.snapshot[key]
      elsif change.respond_to?(key)
        change.public_send(key)
      end
    end

    def drain_solid_queue(limit: 100)
      process = SolidQueue::Process.register(
        kind: "Worker",
        name: "test-worker-#{SecureRandom.hex(8)}",
        pid: ::Process.pid,
        hostname: "localhost",
        metadata: {},
      )
      performed = 0

      loop do
        SolidQueue::ScheduledExecution.dispatch_next_batch(limit)
        claimed = SolidQueue::ReadyExecution.claim("*", limit, process.id)
        break if claimed.empty?

        claimed.each do |execution|
          execution.perform
          performed += 1
        end
      end

      performed
    ensure
      process&.deregister
    end

    def clear_solid_queue_tables
      clear_tables(/\Asolid_queue_/)
    end

    def clear_durable_flow_tables
      clear_tables(/\Adurable_flow_/)
    end

    def clear_tables(pattern)
      connection = ActiveRecord::Base.connection
      tables = connection.tables.grep(pattern)

      connection.disable_referential_integrity do
        tables.each { |table| connection.execute("DELETE FROM #{connection.quote_table_name(table)}") }
      end
    end
end
