# DurableFlow LLM Reference

This document is optimized for coding agents and other LLMs that need to generate, review, or refactor DurableFlow workflows in Rails applications. It describes the public API surface, expected workflow style, lifecycle behavior, and common implementation patterns.

DurableFlow is an in-process, database-backed workflow runtime for Rails. Workflows are Active Job jobs. Durable state is stored in Active Record tables. External wakeups use `Rails.event`. The API is intentionally close to Inngest and Restate concepts while remaining idiomatic Ruby.

## Core Mental Model

A workflow is a Ruby method that can replay from the top. Code outside durable steps runs on every replay. Code inside named durable steps runs at most once after the step result has committed.

```ruby
class TrialOnboardingWorkflow < ApplicationWorkflow
  def perform(user_id:, trial_id:)
    user = step.run(:load_user_snapshot) do
      record = User.find(user_id)
      { "id" => record.id, "email" => record.email }
    end

    step.run(:send_welcome_email) do
      UserMailer.with(user_id: user.fetch("id")).welcome.deliver_now
      true
    end

    step.sleep(:wait_for_trial_activity, 3.days)

    event = step.wait_for_event(
      :trial_activated,
      timeout: 4.days,
      match: { trial_id: trial_id }
    )

    step.run(:mark_onboarded) do
      User.find(user_id).update!(onboarded_at: Time.current, onboarding_source: event[:source])
    end
  end
end
```

Use DurableFlow when a process has multiple side effects, waits, retries, child workflows, human decisions, or external callbacks and needs observable durable progress.

Do not use DurableFlow for a single short synchronous operation that can be represented as a normal method call or one Active Job.

## Installation Surface

Gem choices:

```ruby
gem "durable_flow", git: "https://github.com/skorfmann/durableflow.git", branch: "main"
gem "durable_flow", path: "../durableflow"
gem "durable_flow"
```

Install:

```sh
bundle install
bin/rails generate durable_flow:install
bin/rails db:migrate
```

Recommended Active Job adapter:

```ruby
config.active_job.queue_adapter = :solid_queue
```

Mount the optional UI:

```ruby
mount DurableFlow::Engine => "/durable_flow"
```

The UI denies access by default. Configure one of:

```ruby
# config/initializers/durable_flow.rb
DurableFlow.ui_http_basic_auth = { name: "...", password: "..." }
DurableFlow.ui_base_controller_class = "AdminController" # a controller enforcing authentication
DurableFlow.ui_allow_unauthenticated_access = true # not recommended outside development
```

Configure execution leases:

```ruby
# config/initializers/durable_flow.rb
DurableFlow.execution_lock_ttl = 15.minutes
```

Set `execution_lock_ttl` longer than the longest single workflow step that does not checkpoint. If a step can run longer than the TTL, split it into smaller steps or use a cursor step that checkpoints.

Create an app base workflow:

```ruby
class ApplicationWorkflow < DurableFlow::Workflow
  queue_as :default
end
```

## Public API Surface

### DurableFlow Module

Configuration:

```ruby
DurableFlow.execution_lock_ttl # default: 10.minutes
DurableFlow.execution_lock_ttl = 30.minutes

DurableFlow.live_broadcaster = ->(change) { ... }
DurableFlow.live_subscribers
DurableFlow.event_subscriber
```

Events:

```ruby
DurableFlow.notify(:event_name, key: "value")
DurableFlow.notify(:event_name, { key: "value" })

DurableFlow.subscribe_to_rails_events!
DurableFlow.unsubscribe_from_rails_events!
DurableFlow.record_event?("payment_received")
DurableFlow.database_ready?
```

Workflow lifecycle event constants:

```ruby
DurableFlow::WORKFLOW_COMPLETED_EVENT # "durable_flow.workflow.completed"
DurableFlow::WORKFLOW_FAILED_EVENT    # "durable_flow.workflow.failed"
DurableFlow::WORKFLOW_FINISHED_EVENT  # "durable_flow.workflow.finished"
```

Live updates:

```ruby
subscriber = DurableFlow.on_change { |change| Rails.logger.info(change.type) }
DurableFlow.unsubscribe_from_changes(subscriber)
DurableFlow.reset_live_broadcasters!
DurableFlow.broadcast_change(change)
```

### Workflow Class API

Workflow classes inherit from `DurableFlow::Workflow`, which inherits from `ActiveJob::Base`.

Active Job APIs still apply:

```ruby
class InvoiceWorkflow < ApplicationWorkflow
  queue_as :billing

  retry_on Net::ReadTimeout, wait: 30.seconds, attempts: 5
  discard_on ActiveJob::DeserializationError
end

InvoiceWorkflow.perform_later(invoice_id)
```

DurableFlow records Active Job retries as workflow and step status `retrying`. When retry attempts are exhausted or `discard_on` handles the error, the workflow is marked `failed`.

Instance methods available inside workflows:

```ruby
step
step(:name) { ... }
step(:name, start: 0) { |s| ... }
step(:name, isolated: true) { ... }
checkpoint!
log
workflow_run
```

`step` without arguments returns the `DurableFlow::StepProxy`. Prefer `step.run`, `step.sleep`, `step.wait_for_event`, and `step.invoke` in new workflows.

### Step API

Runtime signatures:

```ruby
step.run(name, start: nil, isolated: false) { ... }
step.sleep(name, duration = nil, **options)                 # options: until:, until_time:
step.sleep_until(name, time)
step.wait_for_event(name, event: nil, timeout: nil, match: {}, allow_past_events: false)
step.wait_for_workflow(name, workflow_or_run_id, timeout: nil)
step.child_workflow(name, workflow_class = nil, *args, timeout: nil, on_failure: :raise, **kwargs) { ... }
step.invoke(name, workflow_class = nil, *args, timeout: nil, on_failure: :raise, **kwargs) { ... }
step.child_workflows(name, collection = nil, key: nil, timeout: nil, concurrency: nil, on_failure: :raise) { ... }
step.invoke_each(name, collection, timeout: nil, concurrency: nil, on_failure: :raise) { |item| ... }
step.each_child_workflow(name, collection, key:, timeout: nil, on_failure: :raise) { |item| ... }
step.workflow(workflow_class, *args, key:, **kwargs)
```

Memoized side-effect step:

```ruby
result = step.run(:create_invoice) do
  Invoice.create!(customer_id: customer.id).id
end
```

Legacy equivalent:

```ruby
result = step(:create_invoice) { Invoice.create!(customer_id: customer.id).id }
```

Method-backed step:

```ruby
def perform(account_id)
  @account_id = account_id
  step(:sync_account)
end

private

def sync_account
  AccountSync.call(@account_id)
end
```

Method-backed steps may accept zero positional arguments or one continuation step argument. They must not accept keyword arguments.

Cursor/checkpoint step:

```ruby
step.run(:sync_accounts, start: 0) do |s|
  Account.where("id >= ?", s.cursor).find_each do |account|
    SyncAccount.call(account)
    s.advance!(from: account.id)
    checkpoint!
  end
end
```

Durable sleep:

```ruby
step.sleep(:retry_tomorrow, 1.day)
step.sleep(:wait_until_send_at, until: campaign.send_at)
step.sleep_until(:wait_until_send_at, campaign.send_at)
```

Wait for event:

```ruby
event = step.wait_for_event(
  :wait_for_payment,
  event: :stripe_payment_succeeded,
  timeout: 2.days,
  match: { invoice_id: invoice_id }
)
```

Event payload matching is recursive subset matching. If the wait matches `{ invoice_id: 123 }`, an event payload `{ invoice_id: 123, provider_id: "evt_1" }` matches. By default, waits ignore historical events that happened before the wait record was created. Use `allow_past_events: true` only when intentional.

Wait for another workflow completion:

```ruby
child = GenerateStatementWorkflow.perform_later(statement.id)
completion = step.wait_for_workflow(:wait_for_statement, child, timeout: 1.hour)
```

Use child workflow APIs instead of raw `wait_for_workflow` when starting and waiting in the same parent workflow.

### Child Workflow APIs

Invoke one child workflow and wait for completion:

```ruby
completion = step.invoke(
  :charge_invoice,
  ChargeInvoiceWorkflow,
  invoice.id,
  timeout: 30.minutes
)
```

Equivalent older name:

```ruby
completion = step.child_workflow(:charge_invoice, ChargeInvoiceWorkflow, invoice.id, timeout: 30.minutes)
```

Keyword arguments pass through to the child workflow:

```ruby
completion = step.invoke(
  :deliver_invoice,
  DeliverInvoiceWorkflow,
  invoice_id: invoice.id,
  channel: "email",
  timeout: 1.hour
)
```

Custom child start block:

```ruby
completion = step.invoke(:deliver_invoice, timeout: 1.hour) do
  DeliverInvoiceWorkflow.perform_later(invoice.id, source: "renewal")
end
```

The custom start block may return an Active Job instance, a run id string, or a hash-like payload containing `run_id` and optionally `workflow_class`.

Child failure policy:

```ruby
# Default. Parent fails with DurableFlow::ChildWorkflowFailedError if child fails.
step.invoke(:charge_invoice, ChargeInvoiceWorkflow, invoice.id, on_failure: :raise)

# Parent receives a failed completion payload and decides what to do.
completion = step.invoke(:charge_invoice, ChargeInvoiceWorkflow, invoice.id, on_failure: :return)
```

Failure completions include fields such as:

```ruby
{
  "run_id" => "...",
  "job_id" => "...",
  "workflow_class" => "ChargeInvoiceWorkflow",
  "status" => "failed",
  "error_class" => "PaymentGateway::Timeout",
  "error_message" => "gateway timeout"
}
```

Fan out to child workflows:

```ruby
completions = step.invoke_each(
  :deliver_invoice,
  invoices,
  timeout: 1.hour,
  concurrency: 25
) do |invoice|
  step.workflow(DeliverInvoiceWorkflow, invoice.id, key: invoice.id)
end
```

`step.workflow(workflow_class, *args, key:, **kwargs)` creates a child workflow request. The `key` must be stable across replays. Fanout creates durable start steps named like `deliver_invoice_123_start` and wait steps named like `deliver_invoice_123_wait`.

Collect partial failures:

```ruby
completions = step.invoke_each(
  :deliver_invoice,
  invoices,
  timeout: 1.hour,
  concurrency: 25,
  on_failure: :return
) do |invoice|
  step.workflow(DeliverInvoiceWorkflow, invoice.id, key: invoice.id)
end

step.run(:summarize_delivery) do
  failed = completions.select { |completion| completion.fetch("status") == "failed" }

  {
    "invoice_count" => completions.size,
    "delivered_count" => completions.count { |completion| completion.fetch("status") == "completed" },
    "failed_count" => failed.size,
    "failed_invoices" => failed.map { |completion| completion.slice("key", "run_id", "error_class", "error_message") }
  }
end
```

Builder form:

```ruby
completions = step.child_workflows(:deliver_invoices, timeout: 1.hour) do |children|
  invoices.each do |invoice|
    children.workflow(DeliverInvoiceWorkflow, invoice.id, key: invoice.id)
  end
end
```

Request object form:

```ruby
class DeliverInvoiceRequest
  def initialize(invoice)
    @invoice = invoice
  end

  def workflow_key = @invoice.id
  def workflow_class = DeliverInvoiceWorkflow
  def workflow_args = [ @invoice.id ]
  def workflow_kwargs = { source: "renewal" }
end

requests = invoices.map { |invoice| DeliverInvoiceRequest.new(invoice) }
completions = step.child_workflows(:deliver_invoice, requests, timeout: 1.hour)
```

Request objects may expose `workflow_arguments` instead of `workflow_kwargs`. A request without `workflow_class` must respond to `perform_later` and return an enqueued child job.

Older fanout API:

```ruby
completions = step.each_child_workflow(:deliver_invoice, invoices, key: ->(invoice) { invoice.id }) do |invoice|
  DeliverInvoiceWorkflow.perform_later(invoice.id)
end
```

Prefer `step.invoke_each` with `step.workflow` for new code because the target workflow and arguments are explicit.

### Logging API

Use structured logs for milestones:

```ruby
step.run(:create_refund) do
  log.info("Creating refund", refund_id: refund.id, amount_cents: refund.amount_cents)
  Refunds.create!(refund)
end

log.warn("Child invoice delivery failed", invoice_id: invoice.id, run_id: completion["run_id"])
log.error("Gateway rejected request", request_id: request_id, error: error.message)
```

Methods:

```ruby
log.debug(message, data_hash = nil, **fields)
log.info(message, data_hash = nil, **fields)
log.warn(message, data_hash = nil, **fields)
log.error(message, data_hash = nil, **fields)
```

Log data must be a Hash and Active Job serializable. Logs written inside a step are linked to that step. Logs written outside a step are run-level logs.

### Definition DAG API

Static analysis:

```ruby
graph = DurableFlow::DefinitionAnalyzer.call(IvwDailyDeliveryWorkflow)
graph.workflow_class
graph.source_file
graph.nodes
graph.edges
graph.warnings
graph.to_h
```

Node fields:

```ruby
node.id
node.type                  # "step", "sleep", "wait_event", "workflow_call", "fanout"
node.name
node.workflow_class
node.target_workflow_class
node.source_file
node.source_line
node.metadata
node.to_h
```

Edge fields:

```ruby
edge.from
edge.to
edge.type                  # usually "sequence"
edge.condition             # branch condition when statically visible
edge.metadata
edge.to_h
```

The analyzer uses source code analysis and does not execute workflow code. It recognizes durable primitives such as `step.run`, `step.sleep`, `step.sleep_until`, `step.wait_for_event`, `step.invoke`, `step.child_workflow`, `step.invoke_each`, `step.child_workflows`, and `step.each_child_workflow`. It warns about hidden durable calls in dynamic loops.

Runtime timelines remain the source of truth for a specific run. Definition DAGs are a static map.

The mountable engine exposes a run-scoped definition graph view:

```text
/durable_flow/workflow_runs/:run_id/definition
```

That page constantizes the run's workflow class, analyzes `#perform`, renders an SVG DAG, and overlays runtime status from the selected run. Child workflow nodes are matched to runtime `_start` / `_wait` steps, and fanout nodes aggregate matching prefixed child steps.

### Timeline API

```ruby
run = DurableFlow::WorkflowRun.find_by!(run_id: params[:run_id])
timeline = run.timeline

timeline.step_entries
timeline.step_entry_for(step_or_id)
timeline.steps
timeline.waits
timeline.events
timeline.logs
timeline.run_logs
timeline.logs_for(step_or_id)
timeline.waits_for(step_or_id)
timeline.items
```

`timeline.items` is a chronological list of `WorkflowTimeline::Item` objects:

```ruby
item.type       # :step, :wait, :event, :log
item.record
item.timestamp
item.step
item.step_id
item.id
item.name
item.status
item.run_level?
```

`timeline.step_entries` returns `WorkflowTimeline::StepEntry` objects:

```ruby
entry.step
entry.logs
entry.waits
entry.events
entry.id
entry.name
entry.status
entry.attempts
```

### Active Record Models

These models are public enough for UI, tests, and operational queries:

```ruby
DurableFlow::WorkflowRun
DurableFlow::WorkflowStep
DurableFlow::WorkflowEvent
DurableFlow::WorkflowWait
DurableFlow::WorkflowLog
```

Workflow run statuses:

```text
enqueued, running, sleeping, waiting, ready, retrying, completed, failed
```

Workflow step statuses:

```text
pending, running, succeeded, sleeping, waiting, retrying, failed
```

Workflow wait statuses:

```text
pending, matched, timed_out
```

Useful model methods:

```ruby
DurableFlow::WorkflowRun.active
run.completed?
run.failed?
run.terminal?
run.execution_locked?
run.timeline
run.live_snapshot

step.succeeded?
step.result_value
step.metadata_hash
step.live_snapshot

DurableFlow::WorkflowEvent.named(:event_name)
event.payload_value
event.matches_payload?({ token: "abc" })
DurableFlow::WorkflowEvent.subset?(expected, actual)

DurableFlow::WorkflowWait.pending
wait.match_value
wait.matches_event?(event)
wait.live_snapshot

DurableFlow::WorkflowLog.ordered
log.data_value
log.live_snapshot
```

Runtime mutator methods such as `WorkflowStep#complete!`, `WorkflowStep#fail!`, `WorkflowStep#retry!`, `WorkflowRun#acquire_execution_lock!`, `WorkflowRun#refresh_execution_lock!`, and `WorkflowRun#release_execution_lock!` are public Ruby methods but should be treated as DurableFlow internals by application workflows.

### Support APIs

Serialization uses Active Job arguments:

```ruby
DurableFlow::Serializer.dump(value)
DurableFlow::Serializer.load(serialized_value)
```

Schema helpers are used by the install migration and tests:

```ruby
DurableFlow::Schema.define
DurableFlow::Schema.define(ActiveRecord::Base.connection)
```

The dispatcher is normally called by DurableFlow event recording. Application code should usually emit events instead of calling the dispatcher directly:

```ruby
DurableFlow::Dispatcher.dispatch(workflow_event)
DurableFlow::Dispatcher.enqueue(workflow_run)
```

### Live Change API

Every broadcasted change has:

```ruby
change.type
change.run_id
change.workflow_class
change.record_class
change.record_id
change.record
change.snapshot
change.payload
```

Configure the host app's broadcaster:

```ruby
DurableFlow.live_broadcaster = ->(change) do
  next unless change.run_id

  ActionCable.server.broadcast(
    "durable_flow:run:#{change.run_id}",
    change.payload
  )
end
```

Broadcast errors are reported through `Rails.error` when available and do not fail workflow execution.

### Errors

```ruby
DurableFlow::Error
DurableFlow::MissingStepResultError
DurableFlow::ChildWorkflowFailedError
DurableFlow::WaitTimeoutError
```

`ChildWorkflowFailedError` fields:

```ruby
error.run_id
error.workflow_class
error.error_class
error.error_message
```

`WaitTimeoutError` fields:

```ruby
error.event_name
error.step_name
```

`DurableFlow::Pause` and `DurableFlow::Interrupt` are internal control-flow exceptions. Do not rescue them in application workflow code.

### Test Helper API

Include:

```ruby
require "durable_flow/test_helper"

class ActiveSupport::TestCase
  include DurableFlow::TestHelper
end
```

Helpers:

```ruby
clear_durable_flow!(clear_jobs: true, reset_live: true)
durable_flow_run(run_id)
durable_flow_run_for(workflow_class)
durable_flow_timeline_for(workflow_or_run)
durable_flow_step(workflow_or_run, name)

perform_durable_flow_jobs(**options)
perform_durable_flow_until_idle(at: Time.current, limit: 100, **options)

notify_workflow_event(name, **payload)
resume_workflows_for(name, **payload)

assert_workflow_completed(workflow_or_run)
assert_workflow_failed(workflow_or_run, error: nil)
assert_workflow_sleeping(workflow_or_run, step: nil)
assert_workflow_waiting_for(workflow_or_run, event_name, match: nil)
assert_workflow_waiting_for_workflow(workflow_or_run, run_id, step: nil)
assert_step_succeeded(workflow_or_run, name)
assert_step_result(workflow_or_run, name, expected)
assert_step_attempts(workflow_or_run, name, expected)
assert_workflow_log(workflow_or_run, level: nil, message: nil, data: nil)
assert_step_log(workflow_or_run, step_name, level: nil, message: nil, data: nil)

travel_to_next_workflow_wake(workflow_or_run = nil)
next_workflow_wake_at(workflow_or_run = nil)

capture_durable_flow_changes { |changes| ... }
assert_durable_flow_change(changes, type, **payload)
```

RSpec suites can include the helper and use the non-assertion lookup/drain helpers directly:

```ruby
RSpec.describe TrialOnboardingWorkflow, type: :job do
  include DurableFlow::TestHelper

  around do |example|
    clear_durable_flow!
    example.run
  ensure
    clear_durable_flow!
  end

  it "does not repeat completed steps on replay" do
    described_class.perform_later(user_id: user.id, trial_id: trial.id)
    perform_durable_flow_until_idle(at: Time.current)

    run = durable_flow_run_for(described_class)
    expect(run.reload.status).to eq("sleeping")
  end
end
```

## Usage Recipes

### External Callback or Webhook

Workflow:

```ruby
class PaymentWorkflow < ApplicationWorkflow
  def perform(invoice_id)
    invoice = step.run(:load_invoice_snapshot) do
      record = Invoice.find(invoice_id)
      {
        "id" => record.id,
        "gateway_customer_id" => record.gateway_customer_id,
        "amount_cents" => record.amount_cents
      }
    end

    request = step.run(:request_payment) do
      Gateway.charge(
        invoice.fetch("gateway_customer_id"),
        amount_cents: invoice.fetch("amount_cents")
      )
    end

    event = step.wait_for_event(
      :wait_for_gateway_result,
      event: :gateway_payment_succeeded,
      timeout: 2.days,
      match: { request_id: request.fetch("request_id") }
    )

    step.run(:mark_paid) do
      Invoice.find(invoice_id).update!(
        paid_at: Time.current,
        gateway_payment_id: event.fetch(:payment_id)
      )
    end
  end
end
```

Webhook controller:

```ruby
class GatewayWebhooksController < ApplicationController
  def create
    payload = Gateway.verify!(request.raw_post)

    Rails.event.notify(
      :gateway_payment_succeeded,
      request_id: payload.fetch("request_id"),
      payment_id: payload.fetch("payment_id")
    )

    head :ok
  end
end
```

Test:

```ruby
PaymentWorkflow.perform_later(invoice.id)
perform_durable_flow_until_idle(at: Time.current)

run = durable_flow_run_for(PaymentWorkflow)
assert_workflow_waiting_for run, :gateway_payment_succeeded, match: { request_id: "req_123" }

resume_workflows_for :gateway_payment_succeeded, request_id: "req_123", payment_id: "pay_123"
assert_workflow_completed run
```

### Human Approval

```ruby
class RefundWorkflow < ApplicationWorkflow
  def perform(refund_id)
    refund = step.run(:load_refund_snapshot) do
      record = Refund.find(refund_id)
      { "id" => record.id, "amount_cents" => record.amount_cents }
    end

    approval = step.run(:create_refund_approval) do
      RefundApproval.create!(refund_id: refund.fetch("id"), token: SecureRandom.hex(16), status: "pending")
    end

    log.info("Waiting for refund approval", refund_id: refund.fetch("id"), approval_id: approval.id)

    decision = step.wait_for_event(
      :wait_for_refund_decision,
      event: :refund_decided,
      timeout: 7.days,
      match: { approval_id: approval.id }
    )

    if decision.fetch(:approved)
      step.run(:issue_refund) { RefundGateway.issue!(Refund.find(refund_id)) }
    else
      step.run(:mark_refund_rejected) { Refund.find(refund_id).update!(status: "rejected") }
    end
  end
end
```

Admin action:

```ruby
def approve
  approval = RefundApproval.find(params[:id])
  approval.update!(status: "approved", decided_by: current_user)

  Rails.event.notify(
    :refund_decided,
    approval_id: approval.id,
    approved: true,
    user_id: current_user.id
  )

  redirect_to approval.refund
end
```

### Child Workflow Fanout With Partial Failure

Use this when each unit should be isolated and retry/fail independently, while the parent should produce a complete report.

```ruby
class DailyDeliveryWorkflow < ApplicationWorkflow
  queue_as :delivery

  def perform(date: Date.yesterday.iso8601)
    delivery_date = Date.iso8601(date)

    units = step.run(:plan_delivery_units) do
      DeliveryPlanner.call(date: delivery_date)
    end

    completions = step.invoke_each(
      :deliver_unit,
      units,
      timeout: 6.hours,
      concurrency: 50,
      on_failure: :return
    ) do |unit|
      step.workflow(
        DeliveryUnitWorkflow,
        unit.fetch("id"),
        date: delivery_date.iso8601,
        key: "#{unit.fetch("id")}-#{delivery_date.iso8601}"
      )
    end

    step.run(:summarize_delivery) do
      failed = completions.select { |completion| completion.fetch("status") == "failed" }

      if failed.any?
        log.warn("Finished daily delivery with failed units", failed_count: failed.size)
      else
        log.info("Finished daily delivery", delivered_count: completions.size)
      end

      {
        "date" => delivery_date.iso8601,
        "unit_count" => completions.size,
        "failed_count" => failed.size,
        "failed_units" => failed.map { |completion| completion.slice("key", "run_id", "error_class", "error_message") }
      }
    end
  end
end
```

### Child Workflow Where Parent Must Fail

Use default `on_failure: :raise` when downstream failure invalidates the parent.

```ruby
class ProvisionAccountWorkflow < ApplicationWorkflow
  def perform(account_id)
    step.invoke(:create_billing_profile, CreateBillingProfileWorkflow, account_id, timeout: 15.minutes)
    step.invoke(:create_crm_record, CreateCrmRecordWorkflow, account_id, timeout: 15.minutes)

    step.run(:activate_account) do
      Account.find(account_id).update!(status: "active")
    end
  end
end
```

### Retryable Step

```ruby
class SyncAccountWorkflow < ApplicationWorkflow
  retry_on Net::OpenTimeout, Net::ReadTimeout, wait: 1.minute, attempts: 5

  def perform(account_id)
    step.run(:push_to_vendor) do
      VendorApi.sync_account!(Account.find(account_id))
    end
  end
end
```

On a retryable failure, the run and failed step become `retrying`; the failed step is attempted again when Active Job retries the workflow. If attempts are exhausted, the run and step become `failed`.

### Explicit Discard

```ruby
class SyncDeletedAccountWorkflow < ApplicationWorkflow
  discard_on ActiveRecord::RecordNotFound do |workflow, error|
    workflow.log.warn("Discarded sync for missing account", error: error.message)
  end

  def perform(account_id)
    step.run(:sync_account) { VendorApi.sync_account!(Account.find(account_id)) }
  end
end
```

`discard_on` marks the DurableFlow run as failed after the discard block runs. Use logs to make intentional discards clear.

### Large Cursor Loop

Use one cursor step when work is sequential and each item is small. Use child workflow fanout when each item is heavy, isolated, or should retry independently.

```ruby
class BackfillAccountsWorkflow < ApplicationWorkflow
  def perform
    step.run(:backfill_accounts, start: 0) do |s|
      Account.where("id > ?", s.cursor).find_each do |account|
        AccountBackfill.call(account)
        s.advance!(from: account.id)
        checkpoint!
      end
    end
  end
end
```

### Backfill With Date Range

```ruby
class MetricsBackfillWorkflow < ApplicationWorkflow
  def perform(start_date:, end_date:)
    range = step.run(:parse_range) do
      {
        "start_date" => Date.iso8601(start_date).iso8601,
        "end_date" => Date.iso8601(end_date).iso8601
      }
    end

    Date.iso8601(range.fetch("start_date")).upto(Date.iso8601(range.fetch("end_date"))) do |date|
      step.run("sync_metrics_#{date.iso8601}") do
        MetricsSync.call(date: date)
      end
    end
  end
end
```

The dynamic step name is acceptable because it is derived from stable input. Do not use random values or current time in step names.

### Durable Delay Before Retry

Use `step.sleep` when the delay is business logic, not error handling.

```ruby
class ReportDeliveryWorkflow < ApplicationWorkflow
  def perform(report_id)
    send_at = step.run(:load_report_send_time) { Report.find(report_id).send_at }

    step.sleep(:wait_until_report_send_time, until: send_at)

    step.run(:deliver_report) { ReportMailer.ready(Report.find(report_id)).deliver_now }
  end
end
```

Use `retry_on` when the delay is caused by an exception that should retry the failed step.

### Definition DAG Export

```ruby
graph = DurableFlow::DefinitionAnalyzer.call(DailyDeliveryWorkflow)
File.write("tmp/daily_delivery_workflow.json", JSON.pretty_generate(graph.to_h))
```

Use DAG output for documentation, review tooling, or an admin visualization. Treat warnings as places where the analyzer could not statically understand dynamic Ruby.

## Generation Rules For LLMs

When generating DurableFlow workflow code:

- Put side effects inside named durable steps.
- Keep code outside steps deterministic and cheap.
- Use stable step names across replays.
- Return only Active Job serializable values from steps.
- Use model ids, strings, numbers, booleans, arrays, and hashes in step results.
- Re-load Active Record models in steps instead of persisting model instances across long waits.
- Use `step.sleep` or `step.sleep_until` for durable business delays.
- Use `step.wait_for_event` for callbacks, webhooks, and human decisions.
- Emit matching `Rails.event.notify` or `DurableFlow.notify` calls where callbacks are received.
- Use `step.invoke` for one downstream workflow.
- Use `step.invoke_each` plus `step.workflow` for fanout.
- Use `on_failure: :raise` when child failure should fail the parent.
- Use `on_failure: :return` when the parent should collect child failures and summarize them.
- Use `log.info`, `log.warn`, and `log.error` with structured fields at important milestones.
- Add tests for replay, sleep, event matching, timeouts, child failure behavior, and retry behavior.

Avoid:

- Random step names.
- Step names based on `Time.current`.
- Side effects outside step blocks.
- Returning Active Record objects from steps unless the app has explicit serialization support and the object remains stable.
- Large unbounded arrays in step results.
- Rescuing DurableFlow internal pause/interrupt exceptions.
- Dynamic loops with hidden durable calls when a static DAG is important; prefer fanout primitives.

## Current Limitations

- DurableFlow does not provide a generic human task system; model human decisions in the host app and resume with events.
- DurableFlow does not provide a cancellation API in the public surface.
- DurableFlow does not provide built-in cron scheduling; use Rails recurring jobs or Solid Queue recurring tasks to start workflows.
- Definition DAGs are conservative static analysis, not runtime traces.
- Steps should still be idempotent where practical. If a process crashes after a side effect runs but before the step result commits, that step can run again.
