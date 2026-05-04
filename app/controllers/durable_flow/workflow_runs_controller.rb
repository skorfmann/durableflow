# frozen_string_literal: true

module DurableFlow
  class WorkflowRunsController < ActionController::Base
    layout false

    def index
      @workflow_runs = WorkflowRun.order(created_at: :desc).limit(100)
    end

    def show
      @workflow_run = WorkflowRun.find_by!(run_id: params[:run_id])
      @workflow_steps = @workflow_run.workflow_steps.order(:created_at)
      @workflow_waits = @workflow_run.workflow_waits.includes(:workflow_event).order(:created_at)
    end
  end
end
