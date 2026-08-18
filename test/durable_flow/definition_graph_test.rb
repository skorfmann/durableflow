# frozen_string_literal: true

require "test_helper"

class DurableFlowDefinitionGraphTest < DurableFlowTestCase
  setup do
    @graph = DurableFlow::DefinitionGraph.new(workflow_class: "MyWorkflow", source_file: "app/workflows/my_workflow.rb")
  end

  test "add_node records the node and returns it" do
    node = add_node(name: :load_user, type: :step, source_line: 4, metadata: { isolated: true, ignored: nil })

    assert_equal [ node ], @graph.nodes
    assert_equal "load_user", node.id
    assert_equal "load_user", node.name
    assert_equal "step", node.type
    assert_equal "MyWorkflow", node.workflow_class
    assert_equal "app/workflows/my_workflow.rb", node.source_file
    assert_equal 4, node.source_line
    assert_equal({ isolated: true }, node.metadata)
    assert_empty @graph.warnings
  end

  test "add_node suffixes duplicate names and warns" do
    first = add_node(name: :load_user, source_line: 4)
    second = add_node(name: :load_user, source_line: 9)
    third = add_node(name: :load_user, source_line: 14)

    assert_equal [ "load_user", "load_user#2", "load_user#3" ], [ first.id, second.id, third.id ]
    assert_equal [ "load_user", "load_user", "load_user" ], @graph.nodes.map(&:name)
    assert_equal [
      "Duplicate durable step name \"load_user\" at app/workflows/my_workflow.rb:9",
      "Duplicate durable step name \"load_user\" at app/workflows/my_workflow.rb:14",
    ], @graph.warnings
  end

  test "add_node falls back to unknown for blank names" do
    node = add_node(name: nil, source_line: 3)

    assert_equal "unknown", node.id
    assert_equal "unknown", node.name
  end

  test "add_edge ignores blank endpoints" do
    @graph.add_edge(from: nil, to: "b")
    @graph.add_edge(from: "a", to: "")

    assert_empty @graph.edges
  end

  test "add_edge records typed edges and drops blank conditions" do
    @graph.add_edge(from: "a", to: "b")
    @graph.add_edge(from: "b", to: "c", type: :branch, condition: "  ", metadata: { kind: "if", extra: nil })
    @graph.add_edge(from: "b", to: "d", type: :branch, condition: "value.present?")

    sequence, blank_condition, conditional = @graph.edges

    assert_equal "sequence", sequence.type
    assert_nil sequence.condition
    assert_nil blank_condition.condition
    assert_equal({ kind: "if" }, blank_condition.metadata)
    assert_equal "value.present?", conditional.condition
    assert_equal "branch", conditional.type
  end

  test "node to_h omits nil values and defaults metadata" do
    node = DurableFlow::DefinitionNode.new(
      id: "a",
      type: "step",
      name: "a",
      workflow_class: "MyWorkflow",
      target_workflow_class: nil,
      source_file: nil,
      source_line: nil,
      metadata: nil,
    )

    assert_equal({ id: "a", type: "step", name: "a", workflow_class: "MyWorkflow", metadata: {} }, node.to_h)
    assert_equal node.to_h, node.as_json
  end

  test "edge to_h omits nil values and defaults metadata" do
    edge = DurableFlow::DefinitionEdge.new(from: "a", to: "b", type: "sequence", condition: nil, metadata: nil)

    assert_equal({ from: "a", to: "b", type: "sequence", metadata: {} }, edge.to_h)
    assert_equal edge.to_h, edge.as_json
  end

  test "graph to_h serializes nodes edges and warnings" do
    add_node(name: :one, source_line: 2)
    add_node(name: :one, source_line: 5)
    @graph.add_edge(from: "one", to: "one#2")

    hash = @graph.to_h

    assert_equal "MyWorkflow", hash[:workflow_class]
    assert_equal "app/workflows/my_workflow.rb", hash[:source_file]
    assert_equal [ "one", "one#2" ], hash[:nodes].map { |node| node[:id] }
    assert_equal [ { from: "one", to: "one#2", type: "sequence", metadata: {} } ], hash[:edges]
    assert_equal 1, hash[:warnings].size
    assert_equal hash, @graph.as_json
  end

  private
    def add_node(name:, type: :step, source_line: 1, target_workflow_class: nil, metadata: {})
      @graph.add_node(
        type: type,
        name: name,
        target_workflow_class: target_workflow_class,
        source_line: source_line,
        metadata: metadata,
      )
    end
end
