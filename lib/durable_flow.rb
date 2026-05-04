# frozen_string_literal: true

require "rails"
require "active_job"
require "active_job/continuation"
require "active_record"
require "active_support/core_ext/numeric/time"
require "active_support/core_ext/object/blank"
require "active_support/core_ext/module/attribute_accessors"
require "securerandom"

require "durable_flow/version"
require "durable_flow/errors"
require "durable_flow/serializer"
require "durable_flow/schema"
require "durable_flow/models/application_record"
require "durable_flow/models/workflow_run"
require "durable_flow/models/workflow_step"
require "durable_flow/models/workflow_event"
require "durable_flow/models/workflow_wait"
require "durable_flow/dispatcher"
require "durable_flow/event_subscriber"
require "durable_flow/step_proxy"
require "durable_flow/workflow"
require "durable_flow/engine"
require "durable_flow/railtie"

module DurableFlow
  mattr_accessor :event_subscriber, default: nil
  mattr_accessor :execution_lock_ttl, default: 10.minutes

  WORKFLOW_COMPLETED_EVENT = "durable_flow.workflow.completed"
  WORKFLOW_FAILED_EVENT = "durable_flow.workflow.failed"

  class << self
    def subscribe_to_rails_events!
      return event_subscriber if event_subscriber
      return unless defined?(Rails) && Rails.respond_to?(:event)

      self.event_subscriber = EventSubscriber.new
      Rails.event.subscribe(event_subscriber) { |event| record_event?(event[:name]) }
      event_subscriber
    end

    def unsubscribe_from_rails_events!
      return unless event_subscriber
      return unless defined?(Rails) && Rails.respond_to?(:event)

      Rails.event.unsubscribe(event_subscriber)
      self.event_subscriber = nil
    end

    def notify(name, payload = nil, **kwargs)
      if defined?(Rails) && Rails.respond_to?(:event)
        payload ? Rails.event.notify(name, payload, **kwargs) : Rails.event.notify(name, **kwargs)
      else
        event = {
          name: name.to_s,
          payload: payload || kwargs,
          tags: {},
          context: {},
          timestamp: Process.clock_gettime(Process::CLOCK_REALTIME, :nanosecond),
        }
        EventSubscriber.new.emit(event)
      end
    end

    def database_ready?
      return false unless defined?(ActiveRecord::Base)
      return false unless ActiveRecord::Base.connected?

      connection = ActiveRecord::Base.connection
      %w[
        durable_flow_workflow_runs
        durable_flow_workflow_steps
        durable_flow_workflow_events
        durable_flow_workflow_waits
      ].all? { |table| connection.data_source_exists?(table) }
    rescue ActiveRecord::ActiveRecordError
      false
    end

    def record_event?(name)
      return false if Fiber[:durable_flow_recording_event]

      name = name.to_s
      return true if name.start_with?("durable_flow.")

      !name.match?(/\.(active_job|active_record|action_controller|action_view|rails)\z/)
    end
  end
end
