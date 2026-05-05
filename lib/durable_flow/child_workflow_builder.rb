# frozen_string_literal: true

module DurableFlow
  class ChildWorkflowBuilder
    Request = Data.define(:workflow_key, :workflow_class, :workflow_args, :workflow_kwargs) do
      def perform_later
        workflow_class.perform_later(*workflow_args, **workflow_kwargs)
      end
    end

    attr_reader :requests

    def initialize
      @requests = []
    end

    def workflow(workflow_class, *args, key:, **kwargs)
      requests << Request.new(
        workflow_key: key.to_s,
        workflow_class: workflow_class,
        workflow_args: args,
        workflow_kwargs: kwargs
      )
    end
  end
end
