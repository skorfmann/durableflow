# frozen_string_literal: true

module DurableFlow
  class StepProxy
    def initialize(workflow)
      @workflow = workflow
    end

    def sleep(name, duration = nil, **options)
      @workflow.sleep_step(name, duration, until_time: options[:until] || options[:until_time])
    end

    def wait_for_event(name, event: nil, timeout: nil, match: {}, allow_past_events: false)
      @workflow.wait_for_event_step(
        name,
        event_name: event || name,
        timeout: timeout,
        match: match,
        allow_past_events: allow_past_events,
      )
    end

    def wait_for_workflow(name, workflow_or_run_id, timeout: nil)
      run_id = workflow_or_run_id.respond_to?(:job_id) ? workflow_or_run_id.job_id : workflow_or_run_id.to_s
      wait_for_event(
        name,
        event: DurableFlow::WORKFLOW_COMPLETED_EVENT,
        timeout: timeout,
        match: { run_id: run_id },
        allow_past_events: true,
      )
    end

    def child_workflow(name, workflow_class = nil, *args, timeout: nil, **kwargs, &block)
      @workflow.child_workflow(name, workflow_class, *args, timeout: timeout, **kwargs, &block)
    end

    def each_child_workflow(name, collection, key:, timeout: nil, &block)
      @workflow.each_child_workflow(name, collection, key: key, timeout: timeout, &block)
    end
  end
end
