# frozen_string_literal: true

module DurableFlow
  class WorkflowRunsController < ActionController::Base
    layout "durable_flow/application"

    helper_method :durable_flow_duration,
      :durable_flow_format_time,
      :durable_flow_json,
      :durable_flow_status_class

    def index
      @workflow_runs = WorkflowRun.order(created_at: :desc).limit(100)
      @status_counts = @workflow_runs.group_by(&:status).transform_values(&:count)
      @active_count = @workflow_runs.count { |run| !run.terminal? }
    end

    def show
      @workflow_run = WorkflowRun.find_by!(run_id: params[:run_id])
      @workflow_steps = @workflow_run.workflow_steps.order(:created_at)
      @workflow_waits = @workflow_run.workflow_waits.includes(:workflow_event).order(:created_at)
      @workflow_logs = @workflow_run.workflow_logs.includes(:workflow_step).ordered
    end

    private
      def durable_flow_status_class(status)
        case status.to_s
        when "completed", "succeeded", "matched"
          "success"
        when "failed", "timed_out"
          "danger"
        when "waiting", "sleeping", "pending"
          "waiting"
        when "running", "ready", "retrying", "enqueued", "info"
          "active"
        when "warn"
          "waiting"
        when "error"
          "danger"
        else
          "neutral"
        end
      end

      def durable_flow_format_time(value)
        return "—" unless value

        value.in_time_zone.strftime("%b %-d, %H:%M:%S %Z")
      end

      def durable_flow_duration(started_at, finished_at = nil)
        return "—" unless started_at

        seconds = ((finished_at || Time.current) - started_at).to_i
        return "#{seconds}s" if seconds < 60

        minutes = seconds / 60
        return "#{minutes}m" if minutes < 60

        hours = minutes / 60
        remaining_minutes = minutes % 60
        remaining_minutes.zero? ? "#{hours}h" : "#{hours}h #{remaining_minutes}m"
      end

      def durable_flow_json(value)
        return "" if value.blank?

        JSON.pretty_generate(value)
      rescue JSON::GeneratorError
        value.inspect
      end
  end
end
