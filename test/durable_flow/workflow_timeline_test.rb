# frozen_string_literal: true

require "test_helper"

class DurableFlowWorkflowTimelineTest < DurableFlowTestCase
  class TimelineWorkflow < DurableFlow::Workflow
    def perform(token)
      log.info("Workflow started", token: token)

      step(:prepare) do
        log.info("Preparing approval wait", token: token)
        { token: token }
      end

      event = step.wait_for_event(:approved, timeout: 1.hour, match: { token: token })

      step(:finish) do
        log.warn("Finishing after approval", token: event[:token])
        event[:token]
      end
    end
  end

  test "groups steps with logs waits and matched events" do
    freeze_time do
      TimelineWorkflow.perform_later("timeline-1")
      perform_enqueued_jobs(at: Time.current)

      run = DurableFlow::WorkflowRun.find_by!(workflow_class: TimelineWorkflow.name)
      timeline = run.timeline

      assert_equal [ "prepare", "approved" ], timeline.step_entries.map(&:name)
      assert_equal [ "Workflow started" ], timeline.run_logs.map(&:message)

      prepare_entry = timeline.step_entry_for(run.workflow_steps.find_by!(name: "prepare"))
      assert_equal [ "Preparing approval wait" ], prepare_entry.logs.map(&:message)
      assert_equal({ token: "timeline-1" }, prepare_entry.logs.first.data_value)
      assert_empty prepare_entry.waits
      assert_empty prepare_entry.events

      wait_entry = timeline.step_entry_for(run.workflow_steps.find_by!(name: "approved").id)
      assert_equal [ "approved" ], wait_entry.waits.map(&:event_name)
      assert_empty wait_entry.logs
      assert_empty wait_entry.events

      assert_timeline_item timeline, :log, "Workflow started", nil
      assert_timeline_item timeline, :log, "Preparing approval wait", prepare_entry.step
      assert_timeline_item timeline, :wait, "approved", wait_entry.step

      Rails.event.notify(:approved, token: "timeline-1")
      perform_enqueued_jobs(at: Time.current)

      completed_timeline = run.reload.timeline
      finish_entry = completed_timeline.step_entry_for(run.workflow_steps.find_by!(name: "finish"))
      wait_entry = completed_timeline.step_entry_for(run.workflow_steps.find_by!(name: "approved"))

      assert_equal [ "Finishing after approval" ], finish_entry.logs.map(&:message)
      assert_equal [ "approved" ], wait_entry.events.map(&:name)
      assert_timeline_item completed_timeline, :event, "approved", wait_entry.step
      assert_timeline_item completed_timeline, :log, "Finishing after approval", finish_entry.step
    end
  end

  private
    def assert_timeline_item(timeline, type, name, step)
      item = timeline.items.find { |candidate| candidate.type == type && candidate.name == name }

      assert item, "Expected timeline item #{type.inspect} #{name.inspect}"
      if step
        assert_equal step, item.step
      else
        assert_nil item.step
      end
    end
end
