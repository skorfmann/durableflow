# frozen_string_literal: true

module DurableFlow
  DefinitionNode = Data.define(
    :id,
    :type,
    :name,
    :workflow_class,
    :target_workflow_class,
    :source_file,
    :source_line,
    :metadata
  ) do
    def to_h
      {
        id: id,
        type: type,
        name: name,
        workflow_class: workflow_class,
        target_workflow_class: target_workflow_class,
        source_file: source_file,
        source_line: source_line,
        metadata: metadata || {}
      }.compact
    end

    alias as_json to_h
  end

  DefinitionEdge = Data.define(:from, :to, :type, :condition, :metadata) do
    def to_h
      {
        from: from,
        to: to,
        type: type,
        condition: condition,
        metadata: metadata || {}
      }.compact
    end

    alias as_json to_h
  end

  class DefinitionGraph
    attr_reader :workflow_class, :source_file, :nodes, :edges, :warnings

    def initialize(workflow_class:, source_file:)
      @workflow_class = workflow_class.to_s
      @source_file = source_file
      @nodes = []
      @edges = []
      @warnings = []
      @node_ids = Hash.new(0)
    end

    def add_node(type:, name:, target_workflow_class:, source_line:, metadata: {})
      node_name = name.to_s.presence || "unknown"
      @node_ids[node_name] += 1
      node_id = @node_ids[node_name] == 1 ? node_name : "#{node_name}##{@node_ids[node_name]}"

      warnings << "Duplicate durable step name #{node_name.inspect} at #{source_file}:#{source_line}" if @node_ids[node_name] > 1

      DefinitionNode.new(
        id: node_id,
        type: type.to_s,
        name: node_name,
        workflow_class: workflow_class,
        target_workflow_class: target_workflow_class,
        source_file: source_file,
        source_line: source_line,
        metadata: metadata.compact
      ).tap { |node| nodes << node }
    end

    def add_edge(from:, to:, type: "sequence", condition: nil, metadata: {})
      return if from.blank? || to.blank?

      edges << DefinitionEdge.new(
        from: from,
        to: to,
        type: type.to_s,
        condition: condition.presence,
        metadata: metadata.compact
      )
    end

    def to_h
      {
        workflow_class: workflow_class,
        source_file: source_file,
        nodes: nodes.map(&:to_h),
        edges: edges.map(&:to_h),
        warnings: warnings
      }
    end

    alias as_json to_h
  end
end
