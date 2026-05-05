# frozen_string_literal: true

require "prism"

module DurableFlow
  class DefinitionAnalyzer
    Path = Data.define(:node_id, :condition)

    STEP_PROXY_CALLS = %i[
      run
      sleep
      sleep_until
      wait_for_event
      invoke
      invoke_each
      call
      call_each
      child_workflow
      child_workflows
      each_child_workflow
    ].freeze

    WORKFLOW_CALLS = %i[invoke call child_workflow].freeze
    FANOUT_CALLS = %i[invoke_each call_each child_workflows each_child_workflow].freeze

    class << self
      def call(workflow_class)
        new(workflow_class).call
      end
    end

    def initialize(workflow_class)
      @workflow_class = workflow_class
      @source_file, @perform_line = workflow_class.instance_method(:perform).source_location
      @graph = DefinitionGraph.new(workflow_class: workflow_class.name, source_file: source_file)
    end

    def call
      result = Prism.parse_file(source_file)
      result.errors.each do |error|
        graph.warnings << "Parse warning at #{source_file}:#{error.location.start_line}: #{error.message}"
      end

      perform_node = find_perform_node(result.value)
      unless perform_node
        graph.warnings << "Could not locate #{workflow_class.name}#perform at #{source_file}:#{perform_line}"
        return graph
      end

      body = perform_node.body&.body || []
      analyze_statements(body, [ Path.new(nil, nil) ])
      graph
    end

    private
      attr_reader :workflow_class, :source_file, :perform_line, :graph

      def analyze_statements(statements, incoming)
        statements.reduce(incoming) do |paths, statement|
          analyze_statement(statement, paths)
        end
      end

      def analyze_statement(statement, incoming)
        case statement
        when Prism::IfNode
          analyze_if(statement, incoming)
        when Prism::ReturnNode
          call_node = durable_call_in(statement)
          call_node ? add_call_node(call_node, incoming).then { [] } : []
        else
          if loop_like?(statement) && durable_call_nested_in?(statement)
            warn_about_hidden_durable_calls(statement)
            return incoming
          end

          if (call_node = durable_call_in(statement))
            add_call_node(call_node, incoming)
          else
            warn_about_hidden_durable_calls(statement)
            incoming
          end
        end
      end

      def analyze_if(if_node, incoming)
        condition = expression_source(if_node.predicate)
        true_paths = incoming.map { |path| path.with(condition: combine_conditions(path.condition, condition)) }
        then_exits = if if_node.statements&.body&.any?
          analyze_statements(if_node.statements.body, true_paths)
        else
          true_paths
        end

        false_condition = negate_condition(condition)
        false_paths = incoming.map { |path| path.with(condition: combine_conditions(path.condition, false_condition)) }
        else_exits = case if_node.subsequent
        when Prism::ElseNode
          if if_node.subsequent.statements&.body&.any?
            analyze_statements(if_node.subsequent.statements.body, false_paths)
          else
            false_paths
          end
        when Prism::IfNode
          analyze_if(if_node.subsequent, false_paths)
        else
          false_paths
        end

        then_exits + else_exits
      end

      def add_call_node(call_node, incoming)
        definition = call_definition(call_node)
        return incoming unless definition

        node = graph.add_node(**definition)
        incoming.each do |path|
          graph.add_edge(from: path.node_id, to: node.id, condition: path.condition)
        end

        [ Path.new(node.id, nil) ]
      end

      def call_definition(call_node)
        arguments = call_arguments(call_node)
        keywords = keyword_arguments(arguments)
        positional = positional_arguments(arguments)
        primitive_name = primitive_name(call_node)

        case primitive_name
        when :step
          durable_step_definition(call_node, positional, keywords)
        when :run
          durable_step_definition(call_node, positional, keywords)
        when :sleep, :sleep_until
          named_definition(call_node, positional, keywords, type: "sleep")
        when :wait_for_event
          named_definition(call_node, positional, keywords, type: "wait_event").tap do |definition|
            definition[:metadata]["event"] = expression_source(keywords["event"]) if keywords["event"]
            definition[:metadata]["match"] = expression_source(keywords["match"]) if keywords["match"]
          end
        when *WORKFLOW_CALLS
          workflow_call_definition(call_node, positional, keywords)
        when *FANOUT_CALLS
          fanout_definition(call_node, positional, keywords)
        end
      end

      def durable_step_definition(call_node, positional, keywords)
        named_definition(call_node, positional, keywords, type: "step")
      end

      def workflow_call_definition(call_node, positional, keywords)
        target = if primitive_name(call_node) == :call
          expression_source(positional.first)
        else
          expression_source(positional[1])
        end
        name_node = keywords["as"] || positional.first

        named_definition(call_node, [ name_node ].compact, keywords, type: "workflow_call").tap do |definition|
          definition[:target_workflow_class] = target
        end
      end

      def fanout_definition(call_node, positional, keywords)
        target = nil
        fanout_source = nil
        key_source = expression_source(keywords["key"])

        if primitive_name(call_node) == :call_each
          target = expression_source(positional.first)
          fanout_source = expression_source(keywords["from"] || positional[1])
        else
          fanout_source = expression_source(positional[1])
          workflow_request = workflow_request_call_in(call_node.block)
          target = expression_source(positional.first) if primitive_name(call_node) == :each_child_workflow
          target ||= expression_source(workflow_request&.arguments&.arguments&.first)
          request_keywords = keyword_arguments(call_arguments(workflow_request))
          key_source ||= expression_source(request_keywords["key"])
        end

        name_node = keywords["as"] || positional.first
        named_definition(call_node, [ name_node ].compact, keywords, type: "fanout").tap do |definition|
          definition[:target_workflow_class] = target
          definition[:metadata]["fanout_source"] = fanout_source if fanout_source.present?
          definition[:metadata]["key"] = key_source if key_source.present?
          if target.blank?
            graph.warnings << "Could not resolve fan-out target workflow at #{source_file}:#{call_node.location.start_line}"
          end
        end
      end

      def named_definition(call_node, positional, keywords, type:)
        name, dynamic_name = durable_name(positional.first)
        metadata = {
          "expression" => expression_source(call_node),
          "dynamic_name" => dynamic_name,
          "timeout" => expression_source(keywords["timeout"]),
          "start" => expression_source(keywords["start"]),
          "isolated" => expression_source(keywords["isolated"])
        }

        if dynamic_name
          graph.warnings << "Could not statically resolve durable step name #{expression_source(positional.first).inspect} at #{source_file}:#{call_node.location.start_line}"
        end

        {
          type: type,
          name: name,
          target_workflow_class: nil,
          source_line: call_node.location.start_line,
          metadata: metadata
        }
      end

      def find_perform_node(node)
        return if node.nil?
        return node if node.is_a?(Prism::DefNode) && node.name == :perform && node.location.start_line == perform_line

        node.child_nodes.compact.each do |child|
          found = find_perform_node(child)
          return found if found
        end

        nil
      end

      def durable_call_in(node)
        return if node.nil? || node.is_a?(Prism::IfNode)
        return node if durable_call?(node)

        node.child_nodes.compact.each do |child|
          found = durable_call_in(child)
          return found if found
        end

        nil
      end

      def warn_about_hidden_durable_calls(statement)
        return unless loop_like?(statement) && durable_call_nested_in?(statement)

        graph.warnings << "Durable step inside dynamic loop at #{source_file}:#{statement.location.start_line}; use step.call_each for graphable fan-out"
      end

      def durable_call_nested_in?(node)
        node.child_nodes.compact.any? do |child|
          durable_call?(child) || durable_call_nested_in?(child)
        end
      end

      def loop_like?(node)
        node.is_a?(Prism::ForNode) ||
          node.is_a?(Prism::WhileNode) ||
          node.is_a?(Prism::UntilNode) ||
          (node.is_a?(Prism::CallNode) && node.block && %i[each map flat_map].include?(node.name))
      end

      def durable_call?(node)
        return false unless node.is_a?(Prism::CallNode)
        return true if node.name == :step && node.receiver.nil?

        step_receiver?(node.receiver) && STEP_PROXY_CALLS.include?(node.name)
      end

      def step_receiver?(receiver)
        receiver.is_a?(Prism::CallNode) && receiver.name == :step && receiver.receiver.nil?
      end

      def primitive_name(call_node)
        call_node.name == :step && call_node.receiver.nil? ? :step : call_node.name
      end

      def workflow_request_call_in(node)
        return if node.nil?

        if node.is_a?(Prism::CallNode) && node.name == :workflow
          return node
        end

        node.child_nodes.compact.each do |child|
          found = workflow_request_call_in(child)
          return found if found
        end

        nil
      end

      def call_arguments(call_node)
        call_node&.arguments&.arguments || []
      end

      def positional_arguments(arguments)
        arguments.reject { |argument| argument.is_a?(Prism::KeywordHashNode) }
      end

      def keyword_arguments(arguments)
        keyword_hash = arguments.find { |argument| argument.is_a?(Prism::KeywordHashNode) }
        return {} unless keyword_hash

        keyword_hash.elements.each_with_object({}) do |element, keywords|
          next unless element.respond_to?(:key) && element.respond_to?(:value)

          keywords[symbol_value(element.key)] = element.value
        end
      end

      def durable_name(node)
        case node
        when Prism::SymbolNode, Prism::StringNode
          [ node.unescaped.to_s, false ]
        when nil
          [ "unknown", true ]
        else
          [ expression_source(node), true ]
        end
      end

      def symbol_value(node)
        return unless node.respond_to?(:unescaped)

        node.unescaped.to_s
      end

      def expression_source(node)
        node&.location&.slice
      end

      def combine_conditions(left, right)
        return right if left.blank?
        return left if right.blank?

        "(#{left}) && (#{right})"
      end

      def negate_condition(condition)
        "!(#{condition})"
      end
  end
end
