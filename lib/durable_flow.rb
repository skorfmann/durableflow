# frozen_string_literal: true

require "rails"
require "active_job"
require "active_job/continuation"
require "active_record"
require "active_support/core_ext/numeric/time"
require "active_support/core_ext/object/blank"
require "active_support/core_ext/hash/indifferent_access"
require "active_support/core_ext/hash/keys"
require "active_support/core_ext/module/attribute_accessors"
require "securerandom"

require "durable_flow/version"
require "durable_flow/errors"
require "durable_flow/serializer"
require "durable_flow/schema"
require "durable_flow/live"
require "durable_flow/definition_graph"
require "durable_flow/definition_analyzer"
require "durable_flow/workflow_logger"
require "durable_flow/workflow_timeline"
require "durable_flow/child_workflow_builder"
require "durable_flow/models/application_record"
require "durable_flow/models/workflow_run"
require "durable_flow/models/workflow_step"
require "durable_flow/models/workflow_event"
require "durable_flow/models/workflow_wait"
require "durable_flow/models/workflow_log"
require "durable_flow/dispatcher"
require "durable_flow/event_subscriber"
require "durable_flow/step_proxy"
require "durable_flow/workflow"
require "durable_flow/engine"
require "durable_flow/railtie"

module DurableFlow
  NOOP_LIVE_BROADCASTER = ->(_change) {}

  DEFAULT_UI_BASE_CONTROLLER_CLASS = "ActionController::Base"

  mattr_accessor :event_subscriber, default: nil
  mattr_accessor :execution_lock_ttl, default: 10.minutes
  mattr_accessor :live_broadcaster, default: NOOP_LIVE_BROADCASTER
  mattr_accessor :live_subscribers, default: []
  mattr_accessor :ui_base_controller_class, default: DEFAULT_UI_BASE_CONTROLLER_CLASS
  mattr_accessor :ui_http_basic_auth, default: nil
  mattr_accessor :ui_allow_unauthenticated_access, default: false

  WORKFLOW_COMPLETED_EVENT = "durable_flow.workflow.completed"
  WORKFLOW_FAILED_EVENT = "durable_flow.workflow.failed"
  WORKFLOW_FINISHED_EVENT = "durable_flow.workflow.finished"
  IGNORED_EVENT_NAMESPACES = %w[
    action_controller
    action_mailbox
    action_mailer
    action_text
    action_view
    active_job
    active_record
    active_storage
    active_support
    rails
  ].freeze

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

    def broadcast_change(change)
      broadcast_live_change(live_broadcaster, change)
      live_subscribers.each { |subscriber| broadcast_live_change(subscriber, change) }
      change
    end

    def on_change(&block)
      raise ArgumentError, "Provide a block" unless block

      self.live_subscribers += [ block ]
      block
    end

    def unsubscribe_from_changes(subscriber)
      self.live_subscribers -= [ subscriber ]
    end

    def reset_live_broadcasters!
      self.live_broadcaster = NOOP_LIVE_BROADCASTER
      self.live_subscribers = []
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
        durable_flow_workflow_logs
      ].all? { |table| connection.data_source_exists?(table) }
    rescue ActiveRecord::ActiveRecordError
      false
    end

    def record_event?(name)
      return false if Fiber[:durable_flow_recording_event]

      name = name.to_s
      return true if name.start_with?("durable_flow.")

      IGNORED_EVENT_NAMESPACES.none? do |namespace|
        name == namespace || name.start_with?("#{namespace}.") || name.end_with?(".#{namespace}")
      end
    end

    private
      def broadcast_live_change(callable, change)
        callable.call(change)
      rescue StandardError => error
        if defined?(Rails) && Rails.respond_to?(:error)
          Rails.error.report(error, handled: true)
        end
      end
  end
end
