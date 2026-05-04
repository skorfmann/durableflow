# DurableFlow

DurableFlow is an Inngest-style workflow runtime for Rails, built on `ActiveJob::Continuable`, Active Record, Solid Queue, and `Rails.event`.

It lets you write long-running business workflows as normal Ruby methods. Side effects live in named `step` blocks. Step return values are persisted, so after a crash, deploy, sleep, event wakeup, or retry, the workflow replays from the top and completed steps return their stored values without running again.

```ruby
class WelcomeWorkflow < DurableFlow::Workflow
  def perform(user_id:, trial_id:)
    user = step(:load_user) { User.find(user_id) }

    step(:send_welcome) { UserMailer.welcome(user).deliver_now }

    step.sleep(:trial_delay, 1.day)

    trial = step(:start_trial) { Billing.start_trial!(user, trial_id:) }

    event = step.wait_for_event(:trial_confirmed, timeout: 7.days, match: { trial_id: trial.id })

    step(:finalize) { user.update!(onboarded_at: Time.current, confirmed_at: event[:confirmed_at]) }
  end
end
```

The goal is durable, observable workflows without a separate workflow server, Redis dependency, or external control plane.

## Status

This is a working prototype targeted at the current vendored Rails `8.2.0.alpha` continuation APIs in `vendor/rails`.

Verified behavior:

- Step return-value memoization across replays.
- Dynamic string step names.
- Cursor-based `ActiveJob::Continuable` loops.
- Durable sleep through `perform_later(wait_until:)`.
- Event waits through `Rails.event`.
- Parent workflows waiting for child workflow completion.
- Solid Queue `1.1.2` integration.
- Database-backed workflow execution leases to prevent concurrent execution of the same run.

## Install

For local development before publishing the gem:

```ruby
# Gemfile
gem "durable_flow", path: "../durableflow"
```

For a published gem:

```ruby
# Gemfile
gem "durable_flow"
```

Then install the tables:

```sh
bundle install
bin/rails generate durable_flow:install
bin/rails db:migrate
```

DurableFlow runs through Active Job. Solid Queue is the recommended adapter:

```ruby
# config/application.rb or config/environments/production.rb
config.active_job.queue_adapter = :solid_queue
```

Mount the optional workflow run viewer:

```ruby
# config/routes.rb
mount DurableFlow::Engine => "/durable_flow"
```

## Configure

Workflow runs use a database-backed execution lease so concurrent workers do not execute the same run at the same time. The lease is refreshed as steps start and continuations checkpoint.

The default TTL is 10 minutes. Set it longer than your longest single step that does not checkpoint:

```ruby
# config/initializers/durable_flow.rb
DurableFlow.execution_lock_ttl = 15.minutes
```

What that means:

```ruby
step(:sync_big_account) do
  SomeApi.sync_everything(account) # If this can take 25 minutes, use a TTL over 25 minutes.
end
```

If you process records in a cursor loop and call `advance!` or `checkpoint!`, the lease refreshes automatically as the loop progresses:

```ruby
step :sync_accounts, start: 0 do |s|
  Account.find_each(start: s.cursor) do |account|
    SyncAccount.call(account)
    s.advance!(from: account.id)
  end
end
```

## Write a Workflow

Create an application base class if you want shared defaults:

```ruby
# app/workflows/application_workflow.rb
class ApplicationWorkflow < DurableFlow::Workflow
  queue_as :default
end
```

Then write workflows as plain Ruby:

```ruby
# app/workflows/trial_onboarding_workflow.rb
class TrialOnboardingWorkflow < ApplicationWorkflow
  def perform(user_id:, trial_id:)
    user = step(:load_user) { User.find(user_id) }

    step(:send_welcome_email) do
      UserMailer.with(user: user).welcome.deliver_now
      true
    end

    step.sleep(:wait_for_trial_activity, 3.days)

    event = step.wait_for_event(
      :trial_activated,
      timeout: 4.days,
      match: { trial_id: trial_id },
    )

    step(:mark_onboarded) do
      user.update!(onboarded_at: Time.current, onboarding_source: event[:source])
    end
  end
end
```

Start it like any Active Job:

```ruby
TrialOnboardingWorkflow.perform_later(user_id: user.id, trial_id: trial.id)
```

Wake it with a Rails event:

```ruby
Rails.event.notify(:trial_activated, trial_id: trial.id, source: "checkout")
```

## Step API

Memoized side-effect step:

```ruby
order = step(:create_order) { Order.create!(cart:) }
```

Durable sleep:

```ruby
step.sleep(:retry_tomorrow, 1.day)
step.sleep(:wait_until_send_at, until: campaign.send_at)
```

Wait for a Rails event:

```ruby
event = step.wait_for_event(:payment_received, timeout: 2.days, match: { invoice_id: invoice.id })
```

Use a different event name than the step name:

```ruby
event = step.wait_for_event(:wait_for_charge, event: :stripe_charge_succeeded, match: { charge_id: charge.id })
```

Wait for a child workflow:

```ruby
child_run_id = step(:start_child) { SendInvoiceWorkflow.perform_later(invoice.id).job_id }
completion = step.wait_for_workflow(:child_finished, child_run_id, timeout: 1.hour)
```

## Iteration Patterns

Small bounded list: one memoized step per item.

```ruby
item_ids.each do |item_id|
  step("notify-#{item_id}") { Notifier.item_ready(item_id).deliver_now }
end
```

Large cursor loop: one continuation step with cursor checkpoints.

```ruby
step :process_orders, start: 0 do |s|
  Order.pending.find_each(start: s.cursor) do |order|
    ProcessOrder.call(order)
    s.advance!(from: order.id)
  end
end
```

Parallel or high-cardinality work: fan out to child workflows.

```ruby
child_run_ids = step(:start_children) do
  account.users.find_each.map { |user| SyncUserWorkflow.perform_later(user.id).job_id }
end

child_run_ids.each do |run_id|
  step.wait_for_workflow("child-#{run_id}", run_id, timeout: 30.minutes)
end
```

## Rules

- Put side effects inside `step` blocks.
- Keep code outside `step` blocks deterministic and cheap. It runs on every replay.
- Step names must be stable across replays. Use `"notify-#{record.id}"`, not `SecureRandom.uuid`.
- Step return values must be Active Job serializable.
- Steps should still be idempotent where practical. If a process crashes after a side effect runs but before the step result commits, that step can run again.
- Set `DurableFlow.execution_lock_ttl` longer than your longest non-checkpointing step.

## Tables

The install generator creates:

- `durable_flow_workflow_runs`
- `durable_flow_workflow_steps`
- `durable_flow_workflow_events`
- `durable_flow_workflow_waits`

Step results and workflow arguments are serialized through Active Job. Event payloads are stored from `Rails.event` notifications and matched against pending waits.

## Testing

Executable examples live in `examples/workflows.rb` and are covered by `test/durable_flow/examples_test.rb`.

Run the suite against the vendored Rails copy:

```sh
mise exec ruby@3.4 -- bundle exec rake test
```

Current suite:

```text
18 runs, 109 assertions, 0 failures, 0 errors, 0 skips
```

## Copyable App Prompt

Paste this into Codex, Claude Code, or another coding agent inside a Rails app:

```text
Add DurableFlow workflows to this Rails application.

Use the DurableFlow gem from:
- local path: ../durableflow
- or published gem: durable_flow

Tasks:
1. Add the gem to the Gemfile and run bundle install.
2. Run bin/rails generate durable_flow:install and migrate the database.
3. Ensure Active Job uses Solid Queue in the relevant environment.
4. Mount DurableFlow::Engine at /durable_flow.
5. Add config/initializers/durable_flow.rb with DurableFlow.execution_lock_ttl set longer than the app's longest non-checkpointing workflow step.
6. Create app/workflows/application_workflow.rb inheriting from DurableFlow::Workflow.
7. Convert this business process into a DurableFlow workflow:
   [Describe the business process here.]
8. Put all side effects inside named step blocks.
9. Use step.sleep for durable delays.
10. Use step.wait_for_event for external callbacks or user actions, and emit matching Rails.event.notify calls from the app code that receives those callbacks.
11. Add tests that prove:
    - completed steps do not run twice on replay,
    - sleep resumes correctly,
    - event waits resume only for matching payloads,
    - timeout behavior is explicit.

Important constraints:
- Step names must be stable across replays.
- Step return values must be Active Job serializable.
- Code outside step blocks must be deterministic and cheap.
- If a single step can run longer than DurableFlow.execution_lock_ttl without checkpointing, increase the TTL or split the work into checkpointed chunks.
```

## What It Is Not

DurableFlow is not trying to replace Temporal or Inngest Cloud. It is an in-app Rails workflow runtime for teams that want durable, observable, multi-step business logic while staying inside Rails, Active Job, and their database.
