# frozen_string_literal: true

module DurableFlow
  class WorkflowRun < ApplicationRecord
    include Live::Broadcastable

    self.table_name = "durable_flow_workflow_runs"

    TERMINAL_STATUSES = %w[completed failed].freeze

    has_many :workflow_steps, class_name: "DurableFlow::WorkflowStep", dependent: :delete_all
    has_many :workflow_waits, class_name: "DurableFlow::WorkflowWait", dependent: :delete_all
    has_many :workflow_logs, class_name: "DurableFlow::WorkflowLog", dependent: :delete_all

    scope :active, -> { where.not(status: TERMINAL_STATUSES) }

    def acquire_execution_lock!(owner:, ttl:)
      now = Time.current
      lock_expires_at = now + ttl

      updated = self.class
        .where(id: id)
        .active
        .where("execution_locked_by IS NULL OR execution_lock_expires_at <= ?", now)
        .update_all(
          execution_locked_by: owner,
          execution_locked_at: now,
          execution_lock_expires_at: lock_expires_at,
          updated_at: now,
        )

      reload if updated == 1
      updated == 1
    end

    def release_execution_lock!(owner:)
      self.class
        .where(id: id, execution_locked_by: owner)
        .update_all(
          execution_locked_by: nil,
          execution_locked_at: nil,
          execution_lock_expires_at: nil,
          updated_at: Time.current,
        )
    end

    def refresh_execution_lock!(owner:, ttl:)
      now = Time.current

      updated = self.class
        .where(id: id, execution_locked_by: owner)
        .update_all(
          execution_lock_expires_at: now + ttl,
          updated_at: now,
        )

      reload if updated == 1
      updated == 1
    end

    def execution_locked?
      execution_locked_by.present? && execution_lock_expires_at.present? && execution_lock_expires_at > Time.current
    end

    def completed?
      status == "completed"
    end

    def failed?
      status == "failed"
    end

    def terminal?
      completed? || failed?
    end

    def live_snapshot
      {
        id: id,
        run_id: run_id,
        job_id: job_id,
        workflow_class: workflow_class,
        status: status,
        queue_name: queue_name,
        priority: priority,
        started_at: started_at,
        interrupted_at: interrupted_at,
        completed_at: completed_at,
        failed_at: failed_at,
        execution_locked: execution_locked?,
        execution_lock_expires_at: execution_lock_expires_at,
        created_at: created_at,
        updated_at: updated_at,
      }
    end
  end
end
