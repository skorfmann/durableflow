# frozen_string_literal: true

require "durable_flow"

module DurableFlowExamples
  class State
    class << self
      attr_accessor :events, :side_effects, :processed_items

      def reset!
        self.events = []
        self.side_effects = []
        self.processed_items = []
      end
    end
  end

  State.reset!

  class WelcomeWorkflow < DurableFlow::Workflow
    def perform(user_id:, trial_id:)
      user = step(:load_user) { { id: user_id, email: "user-#{user_id}@example.test" } }

      step(:send_welcome) do
        State.side_effects << [ :welcome_email, user[:email] ]
        true
      end

      step.sleep(:trial_delay, 10.minutes)

      trial = step(:start_trial) { { id: trial_id, user_id: user[:id] } }

      event = step.wait_for_event(:trial_confirmed, timeout: 1.hour, match: { trial_id: trial[:id] })

      step(:finalize) do
        State.events << [ :onboarded, user[:id], event[:trial_id] ]
        true
      end
    end
  end

  class BatchNotifyWorkflow < DurableFlow::Workflow
    def perform(item_ids)
      item_ids.map do |item_id|
        step("notify-#{item_id}") do
          State.processed_items << item_id
          item_id
        end
      end
    end
  end

  class CursorWorkflow < DurableFlow::Workflow
    def perform(item_ids)
      step :process_items, start: 0 do |s|
        item_ids[s.cursor..].each do |item_id|
          State.processed_items << item_id
          s.advance!
        end
      end
    end
  end

  class ChildWorkflow < DurableFlow::Workflow
    def perform(value)
      step(:work) do
        State.side_effects << [ :child, value ]
        value.upcase
      end
    end
  end

  class ParentWorkflow < DurableFlow::Workflow
    def perform(value)
      child_run_id = step(:start_child) { ChildWorkflow.perform_later(value).job_id }

      completion = step.wait_for_workflow(:child_completed, child_run_id, timeout: 1.hour)

      step(:finish) do
        State.events << [ :parent_finished, completion[:run_id] ]
        true
      end
    end
  end
end
