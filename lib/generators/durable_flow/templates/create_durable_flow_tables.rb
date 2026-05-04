# frozen_string_literal: true

class CreateDurableFlowTables < ActiveRecord::Migration[8.0]
  def change
    create_table :durable_flow_workflow_runs do |t|
      t.string :run_id, null: false
      t.string :job_id, null: false
      t.string :workflow_class, null: false
      t.string :status, null: false, default: "enqueued"
      t.string :queue_name
      t.integer :priority
      t.json :arguments
      t.json :serialized_job
      t.json :last_error
      t.datetime :started_at
      t.datetime :interrupted_at
      t.datetime :completed_at
      t.datetime :failed_at
      t.string :execution_locked_by
      t.datetime :execution_locked_at
      t.datetime :execution_lock_expires_at
      t.timestamps
    end

    add_index :durable_flow_workflow_runs, :run_id, unique: true
    add_index :durable_flow_workflow_runs, [ :workflow_class, :status ], name: "idx_durable_flow_runs_on_class_status"
    add_index :durable_flow_workflow_runs, :execution_lock_expires_at, name: "idx_durable_flow_runs_on_lock_expiry"

    create_table :durable_flow_workflow_steps do |t|
      t.references :workflow_run, null: false, index: false
      t.string :name, null: false
      t.string :status, null: false, default: "pending"
      t.integer :attempts, null: false, default: 0
      t.json :result
      t.json :metadata
      t.datetime :started_at
      t.datetime :completed_at
      t.timestamps
    end

    add_index :durable_flow_workflow_steps,
      [ :workflow_run_id, :name ],
      unique: true,
      name: "idx_durable_flow_steps_on_run_and_name"

    create_table :durable_flow_workflow_events do |t|
      t.string :name, null: false
      t.json :payload
      t.json :tags
      t.json :context
      t.json :source_location
      t.datetime :occurred_at, null: false
      t.timestamps
    end

    add_index :durable_flow_workflow_events, [ :name, :occurred_at ], name: "idx_durable_flow_events_on_name_time"

    create_table :durable_flow_workflow_waits do |t|
      t.references :workflow_run, null: false, index: false
      t.references :workflow_step, null: false, index: false
      t.references :workflow_event, index: false
      t.string :event_name, null: false
      t.string :status, null: false, default: "pending"
      t.json :match
      t.datetime :timeout_at
      t.timestamps
    end

    add_index :durable_flow_workflow_waits,
      [ :workflow_run_id, :workflow_step_id ],
      unique: true,
      name: "idx_durable_flow_waits_on_run_and_step"
    add_index :durable_flow_workflow_waits,
      [ :event_name, :status ],
      name: "idx_durable_flow_waits_on_event_status"

    create_table :durable_flow_workflow_logs do |t|
      t.references :workflow_run, null: false, index: false
      t.references :workflow_step, index: false
      t.string :level, null: false
      t.string :message, null: false
      t.json :data
      t.timestamps
    end

    add_index :durable_flow_workflow_logs,
      [ :workflow_run_id, :created_at ],
      name: "idx_durable_flow_logs_on_run_time"
    add_index :durable_flow_workflow_logs,
      [ :workflow_step_id, :created_at ],
      name: "idx_durable_flow_logs_on_step_time"
  end
end
