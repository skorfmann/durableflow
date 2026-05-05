# frozen_string_literal: true

require "test_helper"

class DurableFlowWorkflowTest < DurableFlowTestCase
  class MemoizedReplayWorkflow < DurableFlow::Workflow
    cattr_accessor :calls, default: 0
    cattr_accessor :seen_values, default: []

    def perform
      token = step(:compute_token) do
        self.class.calls += 1
        "token-#{self.class.calls}"
      end

      step(:use_token) do
        self.class.seen_values << token
      end
    end
  end

  class SleepWorkflow < DurableFlow::Workflow
    cattr_accessor :events, default: []

    def perform
      step(:before_sleep) { self.class.events << :before_sleep }
      step.sleep(:nap, 10.minutes)
      step(:after_sleep) { self.class.events << :after_sleep }
    end
  end

  class SleepUntilWorkflow < DurableFlow::Workflow
    cattr_accessor :events, default: []

    def perform(wake_at)
      step(:before_sleep) { self.class.events << :before_sleep }
      step.sleep(:nap, until: wake_at)
      step(:after_sleep) { self.class.events << :after_sleep }
    end
  end

  class NativeStepApiWorkflow < DurableFlow::Workflow
    cattr_accessor :events, default: []

    def perform(wake_at)
      step.run(:before_sleep) { self.class.events << :before_sleep }
      step.sleep_until(:nap, wake_at)
      step.run(:after_sleep) { self.class.events << :after_sleep }
    end
  end

  class WaitWorkflow < DurableFlow::Workflow
    cattr_accessor :events, default: []

    def perform(token)
      event = step.wait_for_event(:approved, timeout: 1.hour, match: { token: token })

      step(:after_event) do
        self.class.events << [ :approved, event[:token] ]
      end
    end
  end

  class MethodCollisionSleepWorkflow < DurableFlow::Workflow
    def parse_time(_value)
      raise "application helper should not override DurableFlow internals"
    end

    def perform(wake_at)
      step.sleep(:nap, until: wake_at)
    end
  end

  class ChildApiChildWorkflow < DurableFlow::Workflow
    cattr_accessor :calls, default: 0

    def perform(value)
      step(:work) do
        self.class.calls += 1
        value.upcase
      end
    end
  end

  class ChildApiParentWorkflow < DurableFlow::Workflow
    cattr_accessor :events, default: []

    def perform(value)
      completion = step.child_workflow(:child, ChildApiChildWorkflow, value, timeout: 1.hour)

      step(:finish) do
        self.class.events << [ :finished, completion[:run_id] ]
        true
      end
    end
  end

  class InvokeApiParentWorkflow < DurableFlow::Workflow
    cattr_accessor :events, default: []

    def perform(value)
      completion = step.invoke(:child, ChildApiChildWorkflow, value, timeout: 1.hour)

      step.run(:finish) do
        self.class.events << [ :finished, completion[:run_id] ]
        true
      end
    end
  end

  class FastChildApiParentWorkflow < DurableFlow::Workflow
    cattr_accessor :events, default: []

    def perform(value)
      completion = step.child_workflow(:child, timeout: 1.hour) do
        child = ChildApiChildWorkflow.new(value)
        child.perform_now
        child
      end

      step(:finish) do
        self.class.events << [ :finished, completion[:run_id] ]
        true
      end
    end
  end

  class EachChildApiParentWorkflow < DurableFlow::Workflow
    cattr_accessor :events, default: []

    def perform(values)
      completions = step.each_child_workflow(:children, values, key: ->(value) { value }, timeout: 1.hour) do |value|
        child = ChildApiChildWorkflow.new(value)
        child.perform_now
        child
      end

      step(:finish) do
        self.class.events = completions.map { |completion| completion[:run_id] }
        true
      end
    end
  end

  ChildWorkflowRequest = Struct.new(:value, keyword_init: true) do
    def workflow_key
      value
    end

    def workflow_class
      ChildApiChildWorkflow
    end

    def workflow_args
      [ value ]
    end
  end

  class ChildWorkflowsApiParentWorkflow < DurableFlow::Workflow
    cattr_accessor :events, default: []

    def perform(values)
      requests = values.map { |value| ChildWorkflowRequest.new(value: value) }
      completions = step.child_workflows(:children, requests, timeout: 1.hour)

      step(:finish) do
        self.class.events = completions.map { |completion| [ completion.fetch("key"), completion.fetch("result") ] }
        true
      end
    end
  end

  class ChildWorkflowsBuilderParentWorkflow < DurableFlow::Workflow
    cattr_accessor :events, default: []

    def perform(values)
      completions = step.child_workflows(:children, timeout: 1.hour) do |children|
        values.each do |value|
          children.workflow(ChildApiChildWorkflow, value, key: value)
        end
      end

      step(:finish) do
        self.class.events = completions.map { |completion| [ completion.fetch("key"), completion.fetch("result") ] }
        true
      end
    end
  end

  class InvokeEachApiParentWorkflow < DurableFlow::Workflow
    cattr_accessor :events, default: []

    def perform(values)
      completions = step.invoke_each(:children, values, timeout: 1.hour) do |value|
        step.workflow(ChildApiChildWorkflow, value, key: value)
      end

      step.run(:finish) do
        self.class.events = completions.map { |completion| [ completion.fetch("key"), completion.fetch("result") ] }
        true
      end
    end
  end

  class FailedChildApiChildWorkflow < DurableFlow::Workflow
    def perform
      step(:work) { raise "boom" }
    end
  end

  class FailedChildApiParentWorkflow < DurableFlow::Workflow
    def perform
      step.child_workflow(:child, FailedChildApiChildWorkflow, timeout: 1.hour)
    end
  end

  class LeasedWorkflow < DurableFlow::Workflow
    cattr_accessor :calls, default: 0

    def perform
      step(:work) do
        self.class.calls += 1
        true
      end
    end
  end

  test "memoizes step return values across continuation replay" do
    MemoizedReplayWorkflow.calls = 0
    MemoizedReplayWorkflow.seen_values = []

    MemoizedReplayWorkflow.perform_later

    interrupt_job_after_step MemoizedReplayWorkflow, :compute_token do
      assert_enqueued_jobs 1, only: MemoizedReplayWorkflow do
        perform_enqueued_jobs
      end
    end

    assert_equal 1, MemoizedReplayWorkflow.calls
    assert_equal [], MemoizedReplayWorkflow.seen_values

    assert_enqueued_jobs 0, only: MemoizedReplayWorkflow do
      perform_enqueued_jobs
    end

    assert_equal 1, MemoizedReplayWorkflow.calls
    assert_equal [ "token-1" ], MemoizedReplayWorkflow.seen_values

    run = DurableFlow::WorkflowRun.find_by!(workflow_class: MemoizedReplayWorkflow.name)
    assert_equal "completed", run.status
    assert_equal %w[compute_token use_token], run.workflow_steps.order(:created_at).pluck(:name)
    assert_equal "token-1", run.workflow_steps.find_by!(name: "compute_token").result_value
  end

  test "sleep step persists wake time and schedules a resume" do
    SleepWorkflow.events = []

    freeze_time do
      SleepWorkflow.perform_later

      perform_enqueued_jobs(at: Time.current)

      run = DurableFlow::WorkflowRun.find_by!(workflow_class: SleepWorkflow.name)
      assert_equal "sleeping", run.status
      assert_equal [ :before_sleep ], SleepWorkflow.events

      nap = run.workflow_steps.find_by!(name: "nap")
      assert_equal "sleeping", nap.status
      assert_equal 10.minutes.from_now.utc.iso8601(9), nap.metadata_hash.fetch("wake_at")

      scheduled = enqueued_jobs.find { |payload| payload[:job] == SleepWorkflow }
      assert scheduled, "Expected a scheduled SleepWorkflow retry"
      assert_in_delta 10.minutes.from_now.to_f, scheduled[:at], 1

      travel 10.minutes
      perform_enqueued_jobs(at: Time.current)

      run.reload
      assert_equal "completed", run.status
      assert_equal [ :before_sleep, :after_sleep ], SleepWorkflow.events
      assert_equal "succeeded", nap.reload.status
    end
  end

  test "sleep step does not resume before wake time" do
    SleepWorkflow.events = []

    freeze_time do
      SleepWorkflow.perform_later
      perform_enqueued_jobs(at: Time.current)

      run = DurableFlow::WorkflowRun.find_by!(workflow_class: SleepWorkflow.name)
      assert_equal "sleeping", run.status

      travel 9.minutes
      perform_enqueued_jobs(at: Time.current)

      assert_equal "sleeping", run.reload.status
      assert_equal [ :before_sleep ], SleepWorkflow.events

      travel 1.minute
      perform_enqueued_jobs(at: Time.current)

      assert_equal "completed", run.reload.status
      assert_equal [ :before_sleep, :after_sleep ], SleepWorkflow.events
    end
  end

  test "sleep step uses absolute until time" do
    SleepUntilWorkflow.events = []

    freeze_time do
      wake_at = 25.minutes.from_now

      SleepUntilWorkflow.perform_later(wake_at)
      perform_enqueued_jobs(at: Time.current)

      run = DurableFlow::WorkflowRun.find_by!(workflow_class: SleepUntilWorkflow.name)
      nap = run.workflow_steps.find_by!(name: "nap")
      assert_equal "sleeping", run.status
      assert_equal wake_at.utc.iso8601(9), nap.metadata_hash.fetch("wake_at")

      scheduled = enqueued_jobs.find { |payload| payload[:job] == SleepUntilWorkflow }
      assert scheduled, "Expected a scheduled SleepUntilWorkflow retry"
      assert_in_delta wake_at.to_f, scheduled[:at], 1

      travel 25.minutes
      perform_enqueued_jobs(at: Time.current)

      assert_equal "completed", run.reload.status
      assert_equal [ :before_sleep, :after_sleep ], SleepUntilWorkflow.events
    end
  end

  test "sleep step accepts time with zone until time" do
    SleepUntilWorkflow.events = []

    freeze_time do
      wake_at = 15.minutes.from_now.in_time_zone("Eastern Time (US & Canada)")

      SleepUntilWorkflow.perform_later(wake_at)
      perform_enqueued_jobs(at: Time.current)

      run = DurableFlow::WorkflowRun.find_by!(workflow_class: SleepUntilWorkflow.name)
      nap = run.workflow_steps.find_by!(name: "nap")
      assert_equal wake_at.utc.iso8601(9), nap.metadata_hash.fetch("wake_at")

      travel 15.minutes
      perform_enqueued_jobs(at: Time.current)

      assert_equal "completed", run.reload.status
      assert_equal [ :before_sleep, :after_sleep ], SleepUntilWorkflow.events
    end
  end

  test "native step api runs and sleeps until an absolute time" do
    NativeStepApiWorkflow.events = []

    freeze_time do
      wake_at = 12.minutes.from_now

      NativeStepApiWorkflow.perform_later(wake_at)
      perform_enqueued_jobs(at: Time.current)

      run = DurableFlow::WorkflowRun.find_by!(workflow_class: NativeStepApiWorkflow.name)
      assert_equal "sleeping", run.status
      assert_equal [ :before_sleep ], NativeStepApiWorkflow.events
      assert_equal wake_at.utc.iso8601(9), run.workflow_steps.find_by!(name: "nap").metadata_hash.fetch("wake_at")

      travel 12.minutes
      perform_enqueued_jobs(at: Time.current)

      assert_equal "completed", run.reload.status
      assert_equal [ :before_sleep, :after_sleep ], NativeStepApiWorkflow.events
    end
  end

  test "sleep step passes through immediately for past until time" do
    SleepUntilWorkflow.events = []

    freeze_time do
      SleepUntilWorkflow.perform_later(1.minute.ago)
      perform_enqueued_jobs(at: Time.current)

      run = DurableFlow::WorkflowRun.find_by!(workflow_class: SleepUntilWorkflow.name)
      nap = run.workflow_steps.find_by!(name: "nap")
      assert_equal "completed", run.status
      assert_equal "succeeded", nap.status
      assert_nil nap.metadata_hash["wake_at"]
      assert_equal [ :before_sleep, :after_sleep ], SleepUntilWorkflow.events
    end
  end

  test "sleep step replay preserves original wake time" do
    SleepWorkflow.events = []

    freeze_time do
      SleepWorkflow.perform_later
      perform_enqueued_jobs(at: Time.current)

      run = DurableFlow::WorkflowRun.find_by!(workflow_class: SleepWorkflow.name)
      nap = run.workflow_steps.find_by!(name: "nap")
      original_wake_at = nap.metadata_hash.fetch("wake_at")

      replay = ActiveJob::Base.deserialize(run.serialized_job)

      travel 5.minutes
      replay.perform_now

      assert_equal "sleeping", run.reload.status
      assert_equal original_wake_at, nap.reload.metadata_hash.fetch("wake_at")
      assert_equal [ :before_sleep ], SleepWorkflow.events
    end
  end

  test "wait for event persists wait and resumes when Rails event matches" do
    WaitWorkflow.events = []

    freeze_time do
      WaitWorkflow.perform_later("abc")
      perform_enqueued_jobs(at: Time.current)

      run = DurableFlow::WorkflowRun.find_by!(workflow_class: WaitWorkflow.name)
      assert_equal "waiting", run.status

      wait = run.workflow_waits.first
      assert_equal "approved", wait.event_name
      assert_equal({ token: "abc" }, wait.match_value)
      assert_equal 1.hour.from_now, wait.timeout_at

      Rails.event.notify("approved", token: "abc")

      assert_equal "matched", wait.reload.status
      perform_enqueued_jobs(at: Time.current)

      assert_equal [ [ :approved, "abc" ] ], WaitWorkflow.events
      assert_equal "completed", run.reload.status
    end
  end

  test "wait for event ignores historical events by default" do
    freeze_time do
      Rails.event.notify("approved", token: "old")

      travel 1.second
      WaitWorkflow.perform_later("old")
      perform_enqueued_jobs(at: Time.current)

      run = DurableFlow::WorkflowRun.find_by!(workflow_class: WaitWorkflow.name)
      assert_equal "waiting", run.status
      assert_equal "pending", run.workflow_waits.first.status
    end
  end

  test "sleep internals are isolated from application helper method names" do
    freeze_time do
      MethodCollisionSleepWorkflow.perform_later(10.minutes.from_now)

      perform_enqueued_jobs(at: Time.current)

      run = DurableFlow::WorkflowRun.find_by!(workflow_class: MethodCollisionSleepWorkflow.name)
      assert_equal "sleeping", run.status
      assert_equal 10.minutes.from_now.utc.iso8601(9), run.workflow_steps.find_by!(name: "nap").metadata_hash.fetch("wake_at")
    end
  end

  test "child workflow helper starts and waits for fast child completion" do
    ChildApiChildWorkflow.calls = 0
    FastChildApiParentWorkflow.events = []

    FastChildApiParentWorkflow.perform_later("hello")
    2.times { perform_enqueued_jobs(at: Time.current) }

    parent = DurableFlow::WorkflowRun.find_by!(workflow_class: FastChildApiParentWorkflow.name)
    child = DurableFlow::WorkflowRun.find_by!(workflow_class: ChildApiChildWorkflow.name)

    assert_equal "completed", parent.reload.status
    assert_equal "completed", child.status
    assert_equal 1, ChildApiChildWorkflow.calls
    assert_equal [ [ :finished, child.run_id ] ], FastChildApiParentWorkflow.events
    assert_equal %w[child_start child_wait finish], parent.workflow_steps.order(:created_at).pluck(:name)
    assert_equal "matched", parent.workflow_waits.first.status
  end

  test "child workflow helper can start workflow class asynchronously" do
    ChildApiChildWorkflow.calls = 0
    ChildApiParentWorkflow.events = []

    ChildApiParentWorkflow.perform_later("hello")
    3.times { perform_enqueued_jobs(at: Time.current) }

    parent = DurableFlow::WorkflowRun.find_by!(workflow_class: ChildApiParentWorkflow.name)
    child = DurableFlow::WorkflowRun.find_by!(workflow_class: ChildApiChildWorkflow.name)

    assert_equal "completed", parent.reload.status
    assert_equal "completed", child.status
    assert_equal 1, ChildApiChildWorkflow.calls
    assert_equal [ [ :finished, child.run_id ] ], ChildApiParentWorkflow.events
  end

  test "invoke helper starts and waits for a child workflow" do
    ChildApiChildWorkflow.calls = 0
    InvokeApiParentWorkflow.events = []

    InvokeApiParentWorkflow.perform_later("hello")
    3.times { perform_enqueued_jobs(at: Time.current) }

    parent = DurableFlow::WorkflowRun.find_by!(workflow_class: InvokeApiParentWorkflow.name)
    child = DurableFlow::WorkflowRun.find_by!(workflow_class: ChildApiChildWorkflow.name)

    assert_equal "completed", parent.reload.status
    assert_equal "completed", child.status
    assert_equal 1, ChildApiChildWorkflow.calls
    assert_equal [ [ :finished, child.run_id ] ], InvokeApiParentWorkflow.events
    assert_equal %w[child_start child_wait finish], parent.workflow_steps.order(:created_at).pluck(:name)
  end

  test "child workflow helper does not start child twice on replay" do
    ChildApiChildWorkflow.calls = 0
    ChildApiParentWorkflow.events = []

    ChildApiParentWorkflow.perform_later("hello")
    perform_enqueued_jobs(at: Time.current)

    parent = DurableFlow::WorkflowRun.find_by!(workflow_class: ChildApiParentWorkflow.name)

    assert_equal "waiting", parent.status
    assert_equal 0, ChildApiChildWorkflow.calls
    assert_equal [], ChildApiParentWorkflow.events
    assert_equal 1, enqueued_jobs.count { |payload| payload[:job] == ChildApiChildWorkflow }

    ActiveJob::Base.deserialize(parent.reload.serialized_job).perform_now

    assert_equal "waiting", parent.reload.status
    assert_equal 0, ChildApiChildWorkflow.calls
    assert_equal 1, enqueued_jobs.count { |payload| payload[:job] == ChildApiChildWorkflow }

    2.times { perform_enqueued_jobs(at: Time.current) }

    child = DurableFlow::WorkflowRun.find_by!(workflow_class: ChildApiChildWorkflow.name)

    assert_equal "completed", parent.reload.status
    assert_equal "completed", child.status
    assert_equal 1, ChildApiChildWorkflow.calls
    assert_equal [ [ :finished, child.run_id ] ], ChildApiParentWorkflow.events
    assert_equal 1, parent.workflow_steps.find_by!(name: "child_start").attempts
  end

  test "each child workflow helper fans out over stable item keys" do
    ChildApiChildWorkflow.calls = 0
    EachChildApiParentWorkflow.events = []

    EachChildApiParentWorkflow.perform_later(%w[a b])
    perform_enqueued_jobs(at: Time.current)

    parent = DurableFlow::WorkflowRun.find_by!(workflow_class: EachChildApiParentWorkflow.name)
    child_run_ids = DurableFlow::WorkflowRun.where(workflow_class: ChildApiChildWorkflow.name).order(:created_at).pluck(:run_id)

    assert_equal "completed", parent.status
    assert_equal 2, ChildApiChildWorkflow.calls
    assert_equal child_run_ids, EachChildApiParentWorkflow.events
    assert_equal %w[children_a_start children_b_start children_a_wait children_b_wait finish], parent.workflow_steps.order(:created_at).pluck(:name)
  end

  test "child workflows helper starts every child before waiting" do
    ChildApiChildWorkflow.calls = 0
    ChildWorkflowsApiParentWorkflow.events = []

    ChildWorkflowsApiParentWorkflow.perform_later(%w[a b])
    perform_enqueued_jobs(only: ChildWorkflowsApiParentWorkflow, at: Time.current)

    parent = DurableFlow::WorkflowRun.find_by!(workflow_class: ChildWorkflowsApiParentWorkflow.name)

    assert_equal "waiting", parent.status
    assert_equal %w[children_a_start children_b_start children_a_wait], parent.workflow_steps.order(:created_at).pluck(:name)
    assert_equal 2, enqueued_jobs.count { |payload| payload[:job] == ChildApiChildWorkflow }

    3.times { perform_enqueued_jobs(at: Time.current) }

    assert_equal "completed", parent.reload.status
    assert_equal 2, ChildApiChildWorkflow.calls
    assert_equal [ [ "a", "A" ], [ "b", "B" ] ], ChildWorkflowsApiParentWorkflow.events
  end

  test "child workflows helper accepts builder block" do
    ChildApiChildWorkflow.calls = 0
    ChildWorkflowsBuilderParentWorkflow.events = []

    ChildWorkflowsBuilderParentWorkflow.perform_later(%w[a b])
    perform_enqueued_jobs(only: ChildWorkflowsBuilderParentWorkflow, at: Time.current)

    parent = DurableFlow::WorkflowRun.find_by!(workflow_class: ChildWorkflowsBuilderParentWorkflow.name)

    assert_equal "waiting", parent.status
    assert_equal %w[children_a_start children_b_start children_a_wait], parent.workflow_steps.order(:created_at).pluck(:name)
    assert_equal 2, enqueued_jobs.count { |payload| payload[:job] == ChildApiChildWorkflow }

    3.times { perform_enqueued_jobs(at: Time.current) }

    assert_equal "completed", parent.reload.status
    assert_equal [ [ "a", "A" ], [ "b", "B" ] ], ChildWorkflowsBuilderParentWorkflow.events
  end

  test "invoke_each fans out through workflow requests" do
    ChildApiChildWorkflow.calls = 0
    InvokeEachApiParentWorkflow.events = []

    InvokeEachApiParentWorkflow.perform_later(%w[a b])
    perform_enqueued_jobs(only: InvokeEachApiParentWorkflow, at: Time.current)

    parent = DurableFlow::WorkflowRun.find_by!(workflow_class: InvokeEachApiParentWorkflow.name)

    assert_equal "waiting", parent.status
    assert_equal %w[children_a_start children_b_start children_a_wait], parent.workflow_steps.order(:created_at).pluck(:name)
    assert_equal 2, enqueued_jobs.count { |payload| payload[:job] == ChildApiChildWorkflow }

    3.times { perform_enqueued_jobs(at: Time.current) }

    assert_equal "completed", parent.reload.status
    assert_equal 2, ChildApiChildWorkflow.calls
    assert_equal [ [ "a", "A" ], [ "b", "B" ] ], InvokeEachApiParentWorkflow.events
  end

  test "child workflow helper fails parent when child fails" do
    FailedChildApiParentWorkflow.perform_later

    perform_enqueued_jobs(at: Time.current)

    assert_raises RuntimeError do
      perform_enqueued_jobs(at: Time.current)
    end

    assert_raises DurableFlow::ChildWorkflowFailedError do
      perform_enqueued_jobs(at: Time.current)
    end

    parent = DurableFlow::WorkflowRun.find_by!(workflow_class: FailedChildApiParentWorkflow.name)
    child = DurableFlow::WorkflowRun.find_by!(workflow_class: FailedChildApiChildWorkflow.name)

    assert_equal "failed", child.status
    assert_equal "failed", parent.status
    assert_equal DurableFlow::ChildWorkflowFailedError.name, parent.last_error.fetch("class")
  end

  test "framework structured events are not stored as workflow events" do
    assert_not DurableFlow.record_event?("active_record.sql")
    assert_not DurableFlow.record_event?("sql.active_record")
    assert_not DurableFlow.record_event?("active_job.step")
    assert_not DurableFlow.record_event?("perform_start.active_job")
    assert DurableFlow.record_event?("approved")
    assert DurableFlow.record_event?(DurableFlow::WORKFLOW_COMPLETED_EVENT)

    assert_no_difference -> { DurableFlow::WorkflowEvent.count } do
      Rails.event.notify("active_record.sql", sql: "select 1")
      Rails.event.notify("active_job.step", step: "load_user")
    end

    assert_difference -> { DurableFlow::WorkflowEvent.count }, 1 do
      Rails.event.notify("approved", token: "abc")
    end
  end

  test "wait for event times out" do
    WaitWorkflow.events = []

    freeze_time do
      WaitWorkflow.perform_later("missing")
      perform_enqueued_jobs(at: Time.current)

      travel 1.hour + 1.second

      assert_raises DurableFlow::WaitTimeoutError do
        perform_enqueued_jobs(at: Time.current)
      end

      run = DurableFlow::WorkflowRun.find_by!(workflow_class: WaitWorkflow.name)
      assert_equal "failed", run.status
      assert_equal "timed_out", run.workflow_waits.first.status
      assert_equal [], WaitWorkflow.events
    end
  end

  test "workflow run execution lease is released after completion" do
    LeasedWorkflow.calls = 0

    LeasedWorkflow.perform_later
    perform_enqueued_jobs

    run = DurableFlow::WorkflowRun.find_by!(workflow_class: LeasedWorkflow.name)
    assert_equal "completed", run.status
    assert_equal 1, LeasedWorkflow.calls
    assert_nil run.execution_locked_by
    assert_nil run.execution_locked_at
    assert_nil run.execution_lock_expires_at
  end

  test "workflow run execution lease skips duplicate active execution" do
    LeasedWorkflow.calls = 0

    freeze_time do
      LeasedWorkflow.perform_later
      run = DurableFlow::WorkflowRun.find_by!(workflow_class: LeasedWorkflow.name)

      assert run.acquire_execution_lock!(owner: "other-worker", ttl: 5.minutes)
      assert run.reload.execution_locked?

      perform_enqueued_jobs

      assert_equal 0, LeasedWorkflow.calls
      assert_equal "enqueued", run.reload.status
      assert_equal "other-worker", run.execution_locked_by
    end
  end

  test "workflow run execution lease can be recovered after expiry" do
    LeasedWorkflow.calls = 0

    freeze_time do
      LeasedWorkflow.perform_later
      run = DurableFlow::WorkflowRun.find_by!(workflow_class: LeasedWorkflow.name)

      assert run.acquire_execution_lock!(owner: "stale-worker", ttl: 1.second)
      travel 2.seconds

      perform_enqueued_jobs

      assert_equal 1, LeasedWorkflow.calls
      assert_equal "completed", run.reload.status
      assert_nil run.execution_locked_by
    end
  end

  test "workflow run execution lease refresh is limited to the owner" do
    freeze_time do
      LeasedWorkflow.perform_later
      run = DurableFlow::WorkflowRun.find_by!(workflow_class: LeasedWorkflow.name)

      assert run.acquire_execution_lock!(owner: "worker-a", ttl: 1.minute)
      original_expiry = run.execution_lock_expires_at

      travel 30.seconds

      assert_not run.refresh_execution_lock!(owner: "worker-b", ttl: 5.minutes)
      assert_equal original_expiry, run.reload.execution_lock_expires_at

      assert run.refresh_execution_lock!(owner: "worker-a", ttl: 5.minutes)
      assert_in_delta 5.minutes.from_now.to_f, run.execution_lock_expires_at.to_f, 1
    end
  end
end
