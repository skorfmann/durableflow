# frozen_string_literal: true

module DurableFlow
  class StepProxy
    def initialize(workflow)
      @workflow = workflow
    end

    def sleep(name, duration = nil, **options)
      @workflow.sleep_step(name, duration, until_time: options[:until] || options[:until_time])
    end

    def wait_for_event(name, event: nil, timeout: nil, match: {})
      @workflow.wait_for_event_step(name, event_name: event || name, timeout: timeout, match: match)
    end

    def wait_for_workflow(name, workflow_or_run_id, timeout: nil)
      run_id = workflow_or_run_id.respond_to?(:job_id) ? workflow_or_run_id.job_id : workflow_or_run_id.to_s
      wait_for_event(name, event: DurableFlow::WORKFLOW_COMPLETED_EVENT, timeout: timeout, match: { run_id: run_id })
    end
  end
end
