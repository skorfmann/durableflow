# frozen_string_literal: true

require "test_helper"

class DurableFlowDefinitionAnalyzerTest < DurableFlowTestCase
  class DefinitionChildWorkflow < DurableFlow::Workflow
    def perform(value = nil)
      step.run(:work) { value }
    end
  end

  class DefinitionWorkflow < DurableFlow::Workflow
    def perform(sync:, units:)
      schedule = step.run(:schedule) { { "sync" => sync } }

      if sync
        step.invoke(:sync_child, DefinitionChildWorkflow, timeout: 1.hour)
      end

      step.sleep_until(:wait_until_delivery, Time.current)
      event = step.wait_for_event(:approved, event: :approved_event, match: { token: schedule.fetch("token") })

      child_results = step.invoke_each(:deliver_unit, units, timeout: 1.hour) do |unit|
        step.workflow(DefinitionChildWorkflow, unit, key: unit)
      end

      step.run(:finish) { { "event" => event, "children" => child_results } }
    end
  end

  class RestateStyleDefinitionWorkflow < DurableFlow::Workflow
    def perform(units)
      step.call(DefinitionChildWorkflow, as: :sync_child, timeout: 1.hour)
      step.call_each(DefinitionChildWorkflow, as: :deliver_unit, from: units, key: ->(unit) { unit })
    end
  end

  class DynamicLoopWorkflow < DurableFlow::Workflow
    def perform(units)
      units.each do |unit|
        step.run("deliver_#{unit}") { true }
      end
    end
  end

  test "builds a conservative definition DAG from durable step primitives" do
    graph = DurableFlow::DefinitionAnalyzer.call(DefinitionWorkflow)

    assert_equal DefinitionWorkflow.name, graph.workflow_class
    assert_equal %w[
      schedule
      sync_child
      wait_until_delivery
      approved
      deliver_unit
      finish
    ], graph.nodes.map(&:id)

    sync_node = graph.nodes.find { |node| node.id == "sync_child" }
    assert_equal "workflow_call", sync_node.type
    assert_equal "DefinitionChildWorkflow", sync_node.target_workflow_class
    assert_equal "1.hour", sync_node.metadata.fetch("timeout")

    wait_node = graph.nodes.find { |node| node.id == "wait_until_delivery" }
    assert_equal "sleep", wait_node.type

    event_node = graph.nodes.find { |node| node.id == "approved" }
    assert_equal "wait_event", event_node.type
    assert_equal ":approved_event", event_node.metadata.fetch("event")
    assert_match(/token:/, event_node.metadata.fetch("match"))

    fanout_node = graph.nodes.find { |node| node.id == "deliver_unit" }
    assert_equal "fanout", fanout_node.type
    assert_equal "units", fanout_node.metadata.fetch("fanout_source")
    assert_equal "unit", fanout_node.metadata.fetch("key")
    assert_equal "DefinitionChildWorkflow", fanout_node.target_workflow_class
  end

  test "marks branch edges with source conditions and joins afterwards" do
    graph = DurableFlow::DefinitionAnalyzer.call(DefinitionWorkflow)
    edges = graph.edges.map(&:to_h)

    assert_includes edges, { from: "schedule", to: "sync_child", type: "sequence", condition: "sync", metadata: {} }
    assert_includes edges, { from: "sync_child", to: "wait_until_delivery", type: "sequence", metadata: {} }
    assert_includes edges, { from: "schedule", to: "wait_until_delivery", type: "sequence", condition: "!(sync)", metadata: {} }
  end

  test "recognizes Restate-style transparent workflow calls" do
    graph = DurableFlow::DefinitionAnalyzer.call(RestateStyleDefinitionWorkflow)

    assert_equal %w[sync_child deliver_unit], graph.nodes.map(&:id)
    assert_equal [ "workflow_call", "fanout" ], graph.nodes.map(&:type)
    assert_equal [ "DefinitionChildWorkflow", "DefinitionChildWorkflow" ], graph.nodes.map(&:target_workflow_class)
    assert_equal "units", graph.nodes.last.metadata.fetch("fanout_source")
    assert_equal "->(unit) { unit }", graph.nodes.last.metadata.fetch("key")
  end

  test "warns instead of pretending dynamic loops are static DAG nodes" do
    graph = DurableFlow::DefinitionAnalyzer.call(DynamicLoopWorkflow)

    assert_empty graph.nodes
    assert_equal 1, graph.warnings.size
    assert_match(/dynamic loop/, graph.warnings.first)
  end

  class WhileLoopWorkflow < DurableFlow::Workflow
    def perform(units)
      while units.any?
        step.run(:deliver) { units.pop }
      end
    end
  end

  test "warns about durable steps hidden inside while loops" do
    graph = DurableFlow::DefinitionAnalyzer.call(WhileLoopWorkflow)

    assert_empty graph.nodes
    assert_equal 1, graph.warnings.size
    assert_match(/dynamic loop/, graph.warnings.first)
  end

  class BranchingWorkflow < DurableFlow::Workflow
    def perform(mode:)
      step(:start) { mode }

      if mode == :fast
        step.run(:fast_path) { true }
      elsif mode == :slow
        step.run(:slow_path) { true }
      else
        step.run(:fallback) { true }
      end

      step.run(:finish) { true }
    end
  end

  test "tracks conditions across elsif and else branches" do
    graph = DurableFlow::DefinitionAnalyzer.call(BranchingWorkflow)
    edges = graph.edges.map(&:to_h)

    assert_equal %w[start fast_path slow_path fallback finish], graph.nodes.map(&:id)
    assert_equal "step", graph.nodes.first.type

    assert_includes edges, { from: "start", to: "fast_path", type: "sequence", condition: "mode == :fast", metadata: {} }
    assert_includes edges, {
      from: "start",
      to: "slow_path",
      type: "sequence",
      condition: "(!(mode == :fast)) && (mode == :slow)",
      metadata: {},
    }
    assert_includes edges, {
      from: "start",
      to: "fallback",
      type: "sequence",
      condition: "(!(mode == :fast)) && (!(mode == :slow))",
      metadata: {},
    }
    assert_equal [ "fast_path", "slow_path", "fallback" ],
      edges.select { |edge| edge[:to] == "finish" }.map { |edge| edge[:from] }
  end

  class EmptyBranchWorkflow < DurableFlow::Workflow
    def perform(mode:)
      step(:start) { mode }

      if mode == :skip
      else
        step.run(:work) { true }
      end

      step.run(:finish) { true }
    end
  end

  test "keeps incoming paths for empty branches" do
    graph = DurableFlow::DefinitionAnalyzer.call(EmptyBranchWorkflow)
    edges = graph.edges.map(&:to_h)

    assert_equal %w[start work finish], graph.nodes.map(&:id)
    assert_includes edges, { from: "start", to: "finish", type: "sequence", condition: "mode == :skip", metadata: {} }
    assert_includes edges, { from: "work", to: "finish", type: "sequence", metadata: {} }
  end

  class ReturnWorkflow < DurableFlow::Workflow
    def perform(mode:)
      step(:start) { mode }

      return step.run(:early) { true } if mode == :early

      step.run(:finish) { true }
    end
  end

  test "treats a returned durable call as a terminal node" do
    graph = DurableFlow::DefinitionAnalyzer.call(ReturnWorkflow)
    edges = graph.edges.map(&:to_h)

    assert_equal %w[start early finish], graph.nodes.map(&:id)
    assert_includes edges, { from: "start", to: "early", type: "sequence", condition: "mode == :early", metadata: {} }
    assert_equal [ "start" ], edges.select { |edge| edge[:to] == "finish" }.map { |edge| edge[:from] }
  end

  class DynamicNameWorkflow < DurableFlow::Workflow
    def perform(name)
      step.run(name) { true }
    end
  end

  test "warns about durable step names that cannot be resolved statically" do
    graph = DurableFlow::DefinitionAnalyzer.call(DynamicNameWorkflow)
    node = graph.nodes.sole

    assert_equal "name", node.name
    assert_equal true, node.metadata.fetch("dynamic_name")
    assert_match(/Could not statically resolve durable step name/, graph.warnings.sole)
  end

  class UnresolvedFanoutWorkflow < DurableFlow::Workflow
    def perform(units)
      step.child_workflows(:deliver, units) { |unit| unit }
    end
  end

  test "warns when a fan-out target workflow cannot be resolved" do
    graph = DurableFlow::DefinitionAnalyzer.call(UnresolvedFanoutWorkflow)

    assert_equal "fanout", graph.nodes.sole.type
    assert_nil graph.nodes.sole.target_workflow_class
    assert_match(/Could not resolve fan-out target workflow/, graph.warnings.sole)
  end

  class EachChildWorkflow < DurableFlow::Workflow
    def perform(units)
      step.each_child_workflow(DefinitionChildWorkflow, units, key: :id) do |unit|
        DefinitionChildWorkflow.perform_later(unit)
      end
    end
  end

  test "resolves each_child_workflow targets and keys" do
    node = DurableFlow::DefinitionAnalyzer.call(EachChildWorkflow).nodes.sole

    assert_equal "fanout", node.type
    assert_equal "DefinitionChildWorkflow", node.target_workflow_class
    assert_equal "units", node.metadata.fetch("fanout_source")
    assert_equal ":id", node.metadata.fetch("key")
  end

  class PlainRubyWorkflow < DurableFlow::Workflow
    def perform(value)
      other = value.to_s.upcase
      step.run(:work) { other }
    end
  end

  test "ignores statements without durable calls" do
    graph = DurableFlow::DefinitionAnalyzer.call(PlainRubyWorkflow)

    assert_equal %w[work], graph.nodes.map(&:id)
    assert_empty graph.warnings
  end
end
