# frozen_string_literal: true

require "test_helper"
require "action_controller"
require File.expand_path("../../app/controllers/durable_flow/workflow_runs_controller", __dir__)

class DurableFlowWorkflowRunsControllerTest < DurableFlowTestCase
  setup do
    @controller = DurableFlow::WorkflowRunsController.new
  end

  test "node status prefers the step with the same name" do
    assert_equal "succeeded", node_status("charge", { "charge" => "succeeded", "charge_wait" => "waiting" })
  end

  test "node status falls back to the wait step" do
    assert_equal "waiting", node_status("approved", { "approved_wait" => "waiting" })
  end

  test "node status maps a succeeded child start step to running" do
    assert_equal "running", node_status("child", { "child_start" => "succeeded" })
    assert_equal "failed", node_status("child", { "child_start" => "failed" })
  end

  test "node status aggregates fan out steps" do
    assert_equal "failed", node_status("fanout", { "fanout_a_start" => "succeeded", "fanout_b_wait" => "failed" })
    assert_equal "waiting", node_status("fanout", { "fanout_a_wait" => "waiting", "fanout_b_wait" => "succeeded" })
    assert_equal "sleeping", node_status("fanout", { "fanout_a_wait" => "sleeping", "fanout_b_wait" => "succeeded" })
    assert_equal "running", node_status("fanout", { "fanout_a_start" => "pending", "fanout_b_wait" => "succeeded" })
    assert_equal "succeeded", node_status("fanout", { "fanout_a_wait" => "succeeded", "fanout_b_wait" => "succeeded" })
    assert_equal "running", node_status("fanout", { "fanout_a_wait" => "matched" })
  end

  test "node status is defined without any matching step" do
    assert_equal "defined", node_status("charge", {})
    assert_equal "defined", node_status("charge", { "other_wait" => "waiting" })
  end

  test "definition class maps defined nodes to neutral" do
    assert_equal "neutral", send_helper(:durable_flow_definition_class, node("charge"), {})
    assert_equal "success", send_helper(:durable_flow_definition_class, node("charge"), { "charge" => "succeeded" })
    assert_equal "danger", send_helper(:durable_flow_definition_class, node("charge"), { "charge" => "failed" })
  end

  test "status class groups statuses by severity" do
    { "completed" => "success", "succeeded" => "success", "matched" => "success",
      "failed" => "danger", "timed_out" => "danger", "error" => "danger",
      "waiting" => "waiting", "sleeping" => "waiting", "pending" => "waiting", "warn" => "waiting",
      "running" => "active", "ready" => "active", "retrying" => "active", "enqueued" => "active", "info" => "active",
      "unknown" => "neutral", "" => "neutral" }.each do |status, expected|
      assert_equal expected, send_helper(:durable_flow_status_class, status), "status #{status.inspect}"
    end

    assert_equal "success", send_helper(:durable_flow_status_class, :completed)
    assert_equal "neutral", send_helper(:durable_flow_status_class, nil)
  end

  test "node label humanizes node types" do
    assert_equal "wait event", send_helper(:durable_flow_node_label, "wait_event")
    assert_equal "workflow call", send_helper(:durable_flow_node_label, :workflow_call)
    assert_equal "child workflow", send_helper(:durable_flow_node_label, :child_workflow)
    assert_equal "step", send_helper(:durable_flow_node_label, "step")
  end

  test "short condition truncates long conditions" do
    assert_equal "user.active?", send_helper(:durable_flow_definition_short_condition, "user.active?")
    assert_equal "a" * 34, send_helper(:durable_flow_definition_short_condition, "a" * 34)
    assert_equal "#{'a' * 31}…", send_helper(:durable_flow_definition_short_condition, "a" * 40)
    assert_equal "", send_helper(:durable_flow_definition_short_condition, nil)
  end

  test "format time renders a placeholder without a value" do
    assert_equal "—", send_helper(:durable_flow_format_time, nil)
  end

  test "format time renders the value in the current zone" do
    Time.use_zone("UTC") do
      assert_equal "Mar 4, 05:06:07 UTC", send_helper(:durable_flow_format_time, Time.utc(2024, 3, 4, 5, 6, 7))
    end
  end

  test "duration renders a placeholder without a start time" do
    assert_equal "—", send_helper(:durable_flow_duration, nil)
  end

  test "duration renders seconds minutes and hours" do
    started_at = Time.utc(2024, 1, 1)

    assert_equal "0s", duration(started_at, started_at)
    assert_equal "59s", duration(started_at, started_at + 59.seconds)
    assert_equal "1m", duration(started_at, started_at + 1.minute)
    assert_equal "59m", duration(started_at, started_at + 59.minutes)
    assert_equal "1h", duration(started_at, started_at + 1.hour)
    assert_equal "2h 5m", duration(started_at, started_at + 2.hours + 5.minutes)
  end

  test "duration measures against the current time when unfinished" do
    started_at = Time.utc(2024, 1, 1)

    travel_to started_at + 90.seconds do
      assert_equal "1m", duration(started_at)
    end
  end

  test "json pretty prints values and blanks out empty ones" do
    assert_equal "", send_helper(:durable_flow_json, nil)
    assert_equal "", send_helper(:durable_flow_json, {})
    assert_equal %({\n  "a": 1\n}), send_helper(:durable_flow_json, { "a" => 1 })
  end

  test "graph layout centers each rank and sizes the canvas" do
    graph = DurableFlow::DefinitionGraph.new(workflow_class: "MyWorkflow", source_file: "my_workflow.rb")
    first = add_node(graph, "first", 1)
    second = add_node(graph, "second", 2)
    third = add_node(graph, "third", 3)
    graph.add_edge(from: first.id, to: second.id)
    graph.add_edge(from: first.id, to: third.id)

    layout = send_helper(:durable_flow_definition_graph_layout, graph)

    assert_equal 300, layout[:node_width]
    assert_equal 76, layout[:node_height]
    assert_equal 880, layout[:width]
    assert_equal 304, layout[:height]
    assert_equal layout[:width] / 2, layout.dig(:positions, "first", :x) + (layout[:node_width] / 2)
    assert_equal 52, layout.dig(:positions, "first", :y)
    assert_equal 176, layout.dig(:positions, "second", :y)
    assert_equal 176, layout.dig(:positions, "third", :y)
    assert_operator layout.dig(:positions, "second", :x), :<, layout.dig(:positions, "third", :x)
  end

  test "graph layout widens the canvas for wide ranks" do
    graph = DurableFlow::DefinitionGraph.new(workflow_class: "MyWorkflow", source_file: "my_workflow.rb")
    root = add_node(graph, "root", 1)
    3.times { |index| graph.add_edge(from: root.id, to: add_node(graph, "child-#{index}", index + 2).id) }

    layout = send_helper(:durable_flow_definition_graph_layout, graph)

    assert_equal 1128, layout[:width]
    assert_equal 3, layout[:positions].values.count { |position| position[:y] == 176 }
  end

  test "graph layout handles graphs without edges" do
    graph = DurableFlow::DefinitionGraph.new(workflow_class: "MyWorkflow", source_file: "my_workflow.rb")
    add_node(graph, "only", 1)

    layout = send_helper(:durable_flow_definition_graph_layout, graph)

    assert_equal 260, layout[:height]
    assert_equal 52, layout.dig(:positions, "only", :y)
  end

  test "graph layout tolerates cyclic edges" do
    graph = DurableFlow::DefinitionGraph.new(workflow_class: "MyWorkflow", source_file: "my_workflow.rb")
    first = add_node(graph, "first", 1)
    second = add_node(graph, "second", 2)
    graph.add_edge(from: first.id, to: second.id)
    graph.add_edge(from: second.id, to: first.id)
    graph.add_edge(from: first.id, to: "missing")

    layout = send_helper(:durable_flow_definition_graph_layout, graph)

    assert_equal 2, layout[:positions].size
  end

  private
    def send_helper(name, *args)
      @controller.send(name, *args)
    end

    def node_status(name, statuses)
      send_helper(:durable_flow_definition_node_status, node(name), statuses)
    end

    def duration(started_at, finished_at = nil)
      send_helper(:durable_flow_duration, started_at, finished_at)
    end

    def node(name)
      DurableFlow::DefinitionNode.new(
        id: name,
        type: "step",
        name: name,
        workflow_class: "MyWorkflow",
        target_workflow_class: nil,
        source_file: "my_workflow.rb",
        source_line: 1,
        metadata: {},
      )
    end

    def add_node(graph, name, source_line)
      graph.add_node(type: :step, name: name, target_workflow_class: nil, source_line: source_line)
    end
end
