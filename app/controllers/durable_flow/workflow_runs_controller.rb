# frozen_string_literal: true

module DurableFlow
  class WorkflowRunsController < DurableFlow.ui_base_controller_class.constantize
    layout "durable_flow/application"

    before_action :durable_flow_authenticate!

    helper_method :durable_flow_duration,
      :durable_flow_definition_class,
      :durable_flow_definition_graph_layout,
      :durable_flow_definition_node_status,
      :durable_flow_definition_short_condition,
      :durable_flow_format_time,
      :durable_flow_json,
      :durable_flow_node_label,
      :durable_flow_status_class

    def index
      @workflow_runs = WorkflowRun.order(created_at: :desc).limit(100)
      @status_counts = @workflow_runs.group_by(&:status).transform_values(&:count)
      @active_count = @workflow_runs.count { |run| !run.terminal? }
    end

    def show
      @workflow_run = WorkflowRun.find_by!(run_id: params[:run_id])
      @workflow_timeline = @workflow_run.timeline
    end

    def definition
      @workflow_run = WorkflowRun.find_by!(run_id: params[:run_id])
      @workflow_class = @workflow_run.workflow_class.safe_constantize
      @workflow_timeline = @workflow_run.timeline
      @step_statuses_by_name = @workflow_run.workflow_steps.index_by(&:name).transform_values(&:status)

      if @workflow_class.nil?
        @definition_error = "Could not constantize #{@workflow_run.workflow_class.inspect}"
      elsif !(@workflow_class <= DurableFlow::Workflow)
        @definition_error = "#{@workflow_run.workflow_class} is not a DurableFlow::Workflow"
      else
        @definition_graph = DurableFlow::DefinitionAnalyzer.call(@workflow_class)
      end
    rescue StandardError => error
      @definition_error = "#{error.class}: #{error.message}"
    end

    private
      def durable_flow_authenticate!
        credentials = DurableFlow.ui_http_basic_auth

        if credentials.present?
          expected_name = (credentials[:name] || credentials["name"]).to_s
          expected_password = (credentials[:password] || credentials["password"]).to_s

          authenticate_or_request_with_http_basic("DurableFlow") do |name, password|
            ActiveSupport::SecurityUtils.secure_compare(name.to_s, expected_name) &
              ActiveSupport::SecurityUtils.secure_compare(password.to_s, expected_password)
          end
        elsif !durable_flow_unauthenticated_access_allowed?
          render plain: <<~MESSAGE, status: :forbidden
            Access to the DurableFlow UI is denied by default.

            Configure one of the following in an initializer:

              DurableFlow.ui_http_basic_auth = { name: "...", password: "..." }
              DurableFlow.ui_base_controller_class = "YourAuthenticatedController"
              DurableFlow.ui_allow_unauthenticated_access = true # not recommended
          MESSAGE
        end
      end

      def durable_flow_unauthenticated_access_allowed?
        DurableFlow.ui_allow_unauthenticated_access ||
          DurableFlow.ui_base_controller_class != DurableFlow::DEFAULT_UI_BASE_CONTROLLER_CLASS
      end

      def durable_flow_definition_node_status(node, statuses_by_name)
        direct_status = statuses_by_name[node.name]
        return direct_status if direct_status

        wait_status = statuses_by_name["#{node.name}_wait"]
        return wait_status if wait_status

        start_status = statuses_by_name["#{node.name}_start"]
        return durable_flow_child_start_status(start_status) if start_status

        fanout_statuses = statuses_by_name.filter_map do |step_name, status|
          status if step_name.match?(/\A#{Regexp.escape(node.name)}_.+_(start|wait)\z/)
        end
        return "defined" if fanout_statuses.empty?

        durable_flow_aggregate_status(fanout_statuses)
      end

      def durable_flow_child_start_status(status)
        status == "succeeded" ? "running" : status
      end

      def durable_flow_aggregate_status(statuses)
        return "failed" if statuses.include?("failed")
        return "waiting" if statuses.include?("waiting")
        return "sleeping" if statuses.include?("sleeping")
        return "running" if statuses.any? { |status| %w[pending running retrying ready enqueued].include?(status) }
        return "succeeded" if statuses.all? { |status| status == "succeeded" }

        "running"
      end

      def durable_flow_definition_class(node, statuses_by_name)
        status = durable_flow_definition_node_status(node, statuses_by_name)
        status == "defined" ? "neutral" : durable_flow_status_class(status)
      end

      def durable_flow_node_label(type)
        case type.to_s
        when "wait_event"
          "wait event"
        when "workflow_call"
          "workflow call"
        else
          type.to_s.tr("_", " ")
        end
      end

      def durable_flow_definition_short_condition(condition)
        condition = condition.to_s
        return condition if condition.length <= 34

        "#{condition.first(31)}…"
      end

      def durable_flow_definition_graph_layout(graph)
        nodes = graph.nodes
        node_width = 300
        node_height = 76
        column_gap = 58
        row_gap = 124
        padding_x = 56
        padding_y = 52
        ranks = durable_flow_definition_node_ranks(graph)
        nodes_by_rank = nodes.group_by { |node| ranks.fetch(node.id, 0) }
        max_columns = [ nodes_by_rank.values.map(&:size).max.to_i, 1 ].max
        width = [ 880, (padding_x * 2) + (max_columns * node_width) + ((max_columns - 1) * column_gap) ].max

        positions = {}
        nodes_by_rank.keys.sort.each do |rank|
          ranked_nodes = nodes_by_rank.fetch(rank).sort_by { |node| [ node.source_line || 0, node.id ] }
          row_width = (ranked_nodes.size * node_width) + ([ ranked_nodes.size - 1, 0 ].max * column_gap)
          start_x = (width - row_width) / 2

          ranked_nodes.each_with_index do |node, index|
            positions[node.id] = {
              x: start_x + (index * (node_width + column_gap)),
              y: padding_y + (rank * row_gap),
            }
          end
        end

        max_rank = ranks.values.max.to_i
        {
          width: width,
          height: [ (padding_y * 2) + node_height + (max_rank * row_gap), 260 ].max,
          node_width: node_width,
          node_height: node_height,
          positions: positions,
        }
      end

      def durable_flow_definition_node_ranks(graph)
        ranks = graph.nodes.to_h { |node| [ node.id, 0 ] }

        graph.nodes.size.times do
          changed = false

          graph.edges.each do |edge|
            next unless ranks.key?(edge.from) && ranks.key?(edge.to)

            next_rank = ranks.fetch(edge.from) + 1
            if next_rank > ranks.fetch(edge.to)
              ranks[edge.to] = next_rank
              changed = true
            end
          end

          break unless changed
        end

        ranks
      end

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
