# frozen_string_literal: true

ENV["RAILS_ENV"] ||= "development"

require "bundler/setup"
require "action_controller/railtie"
require "active_job/railtie"
require "active_job/test_helper"
require "durable_flow"
require "erb"
require "fileutils"
require "json"
require "logger"
require "rack/mock"
require "securerandom"
require "socket"

ROOT = File.expand_path(__dir__)
DB_PATH = File.join(ROOT, "db/live.sqlite3")
CLIENTS = []
CLIENTS_MUTEX = Mutex.new

FileUtils.mkdir_p(File.dirname(DB_PATH))

class DurableFlowLiveDemoApp < Rails::Application
  config.root = ROOT
  config.secret_key_base = "durable-flow-live-demo-secret-key-base"
  config.eager_load = false
  config.consider_all_requests_local = true
  config.active_job.queue_adapter = :test
  config.logger = Logger.new(IO::NULL)
  config.hosts.clear

  routes.append do
    mount DurableFlow::Engine => "/durable_flow"
  end
end

ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: DB_PATH, pool: 10)
ActiveRecord.default_timezone = :utc

DurableFlowLiveDemoApp.initialize!
DurableFlow::Schema.define
DurableFlow.unsubscribe_from_rails_events!
DurableFlow.subscribe_to_rails_events!

class LiveRiskAssessmentWorkflow < DurableFlow::Workflow
  def perform(review_id:, amount_cents:)
    score = step(:score_request) do
      score = amount_cents >= 100_000 ? 91 : 18
      log.info("Scored review request", review_id: review_id, amount_cents: amount_cents, risk_score: score)

      {
        review_id: review_id,
        amount_cents: amount_cents,
        risk_score: score,
        requires_review: score >= 80,
      }
    end

    step(:record_risk_decision) do
      log.info("Recorded risk decision", score)
      score
    end
  end
end

class LivePaymentCaptureWorkflow < DurableFlow::Workflow
  def perform(review_id:, amount_cents:)
    authorization = step(:authorize_payment) do
      log.info("Authorizing payment", review_id: review_id, amount_cents: amount_cents)

      {
        review_id: review_id,
        amount_cents: amount_cents,
        authorization_id: "auth_#{review_id.delete("-")}",
      }
    end

    step(:capture_payment) do
      log.info("Capturing payment", authorization_id: authorization.fetch(:authorization_id))

      {
        review_id: review_id,
        payment_id: "pay_#{review_id.delete("-")}",
        captured_cents: amount_cents,
      }
    end
  end
end

class LiveReviewWorkflow < DurableFlow::Workflow
  def perform(review_id, amount_cents: 124_000)
    request = step(:create_review_request) do
      log.info("Creating review request", review_id: review_id, amount_cents: amount_cents)

      {
        id: review_id,
        amount_cents: amount_cents,
        customer: "Northwind",
      }
    end

    risk_completion = step.invoke(
      :assess_risk,
      LiveRiskAssessmentWorkflow,
      review_id: request.fetch(:id),
      amount_cents: request.fetch(:amount_cents),
      timeout: 1.hour
    )
    risk = risk_completion.fetch(:result)

    if risk.fetch(:requires_review)
      step(:notify_reviewer) do
        log.info(
          "Waiting for human decision",
          review_id: request.fetch(:id),
          channel: "demo-ui",
          risk_score: risk.fetch(:risk_score),
        )
        true
      end

      decision = step.wait_for_event(:approval_decided, match: { review_id: request.fetch(:id) })

      approval = step(:apply_manual_decision) do
        approved = decision.fetch(:decision) == "approved"

        log.warn(
          "Applying human decision",
          review_id: request.fetch(:id),
          approved: approved,
          decision: decision.fetch(:decision),
          decided_by: decision.fetch(:decided_by),
        )

        {
          review_id: request.fetch(:id),
          approved: approved,
          decision: decision.fetch(:decision),
          decided_by: decision.fetch(:decided_by),
        }
      end
    else
      approval = step(:auto_approve_request) do
        log.info("Auto-approving low-risk request", review_id: request.fetch(:id), risk_score: risk.fetch(:risk_score))

        {
          review_id: request.fetch(:id),
          approved: true,
          decision: "auto_approved",
          risk_score: risk.fetch(:risk_score),
        }
      end
    end

    if approval.fetch(:approved)
      payment = step.invoke(
        :capture_payment,
        LivePaymentCaptureWorkflow,
        review_id: request.fetch(:id),
        amount_cents: request.fetch(:amount_cents),
        timeout: 1.hour
      )

      step(:release_order) do
        log.info("Released approved order", review_id: request.fetch(:id), payment_run_id: payment.fetch(:run_id))

        {
          review_id: request.fetch(:id),
          status: "released",
          payment: payment.fetch(:result),
        }
      end
    else
      step(:cancel_request) do
        log.warn("Canceled rejected request", review_id: request.fetch(:id), decision: approval.fetch(:decision))

        {
          review_id: request.fetch(:id),
          status: "canceled",
          decision: approval.fetch(:decision),
        }
      end
    end
  end
end

DAG_WORKFLOWS = [
  [ "Risk-Gated Review Workflow", LiveReviewWorkflow ],
].freeze

DurableFlow.live_broadcaster = lambda do |change|
  payload = normalize_json(change.payload)
  payload.merge!(display_payload_for(change.record))

  CLIENTS_MUTEX.synchronize do
    CLIENTS.each { |queue| queue << payload }
  end
end

def clear_demo_data!
  [
    DurableFlow::WorkflowLog,
    DurableFlow::WorkflowWait,
    DurableFlow::WorkflowEvent,
    DurableFlow::WorkflowStep,
    DurableFlow::WorkflowRun,
  ].each(&:delete_all)
end

def current_run
  DurableFlow::WorkflowRun
    .where(workflow_class: LiveReviewWorkflow.name)
    .order(created_at: :desc, id: :desc)
    .first
end

def start_demo_run!(amount_cents: 124_000)
  clear_test_jobs!
  LiveReviewWorkflow.perform_later("review-#{SecureRandom.hex(3)}", amount_cents: amount_cents)
  drain_demo_jobs!
end

def reset_demo!
  clear_demo_data!
  clear_test_jobs!
end

def complete_demo_run!(decision)
  run = current_run
  return unless run&.status == "waiting"

  request_step = run.workflow_steps.find_by(name: "create_review_request")
  return unless request_step

  review_id = request_step.result_value.fetch(:id)

  Rails.event.notify(
    :approval_decided,
    review_id: review_id,
    decision: decision,
    decided_by: "reviewer@example.test",
  )

  drain_demo_jobs!
  clear_test_jobs!
end

def drain_demo_jobs!(limit: 50)
  adapter = ActiveJob::Base.queue_adapter
  return unless adapter.respond_to?(:enqueued_jobs)

  limit.times do
    payload = adapter.enqueued_jobs.shift
    break unless payload

    ActiveJob::Base.deserialize(payload).perform_now
  end
end

def clear_test_jobs!
  adapter = ActiveJob::Base.queue_adapter
  adapter.enqueued_jobs.clear if adapter.respond_to?(:enqueued_jobs)
  adapter.performed_jobs.clear if adapter.respond_to?(:performed_jobs)
end

def normalize_json(value)
  case value
  when Hash
    value.each_with_object({}) { |(key, nested), result| result[key.to_s] = normalize_json(nested) }
  when Array
    value.map { |nested| normalize_json(nested) }
  when Time, Date, DateTime
    value.iso8601
  else
    value
  end
end

def safe_value
  yield
rescue StandardError
  nil
end

def display_payload_for(record)
  case record
  when DurableFlow::WorkflowStep
    {
      "display_result" => safe_value { normalize_json(record.result_value) },
      "display_metadata" => normalize_json(record.metadata_hash.presence),
    }.compact
  when DurableFlow::WorkflowWait
    { "display_match" => safe_value { normalize_json(record.match_value) } }.compact
  when DurableFlow::WorkflowLog
    {
      "display_data" => safe_value { normalize_json(record.data_value) },
      "step_name" => record.workflow_step&.name,
    }.compact
  else
    {}
  end
end

def status_class(status)
  case status.to_s
  when "completed", "succeeded", "matched"
    "success"
  when "failed", "timed_out", "error"
    "danger"
  when "waiting", "sleeping", "pending", "warn"
    "waiting"
  else
    "active"
  end
end

def step_to_json(step)
  {
    id: step.id,
    name: step.name,
    status: step.status,
    attempts: step.attempts,
    display_result: safe_value { normalize_json(step.result_value) },
    display_metadata: normalize_json(step.metadata_hash.presence),
  }.compact
end

def log_to_json(log)
  {
    id: log.id,
    workflow_step_id: log.workflow_step_id,
    level: log.level,
    message: log.message,
    display_data: safe_value { normalize_json(log.data_value) },
    step_name: log.workflow_step&.name,
    created_at: log.created_at&.iso8601,
  }.compact
end

def wait_summary(waits)
  waits.map { |wait| "#{wait.event_name}: #{wait.status}" }.join(", ").presence || "-"
end

def render_json_block(value)
  return "" if value.blank?

  "<pre>#{ERB::Util.html_escape(JSON.pretty_generate(value))}</pre>"
end

def render_step_logs(logs)
  return "" if logs.empty?

  <<~HTML
    <div class="step-logs">
      <div class="step-logs-label">Logs</div>
      #{logs.map do |log|
        <<~LOG
          <div class="step-log">
            <div class="step-log-top">
              <span class="status #{status_class(log.level)}">#{ERB::Util.html_escape(log.level)}</span>
              <span class="step-log-message">#{ERB::Util.html_escape(log.message)}</span>
            </div>
            #{render_json_block(safe_value { normalize_json(log.data_value) })}
          </div>
        LOG
      end.join}
    </div>
  HTML
end

def render_steps(step_entries)
  return '<div class="empty">No steps yet. Start a run.</div>' if step_entries.empty?

  step_entries.map do |entry|
    step = entry.step
    payload = safe_value { step.result_value } || step.metadata_hash.presence
    cls = status_class(step.status)

    <<~HTML
      <article class="step">
        <div class="rail"><span class="dot #{cls}"></span></div>
        <div class="step-body">
          <div class="step-top">
            <div>
              <div class="step-name">#{ERB::Util.html_escape(step.name)}</div>
              <div class="meta">Attempt #{step.attempts}</div>
            </div>
            <span class="status #{cls}">#{ERB::Util.html_escape(step.status)}</span>
          </div>
          #{render_json_block(normalize_json(payload))}
          #{render_step_logs(entry.logs)}
        </div>
      </article>
    HTML
  end.join
end

def current_step_statuses_for(workflow_class)
  run = DurableFlow::WorkflowRun
    .where(workflow_class: workflow_class.name)
    .order(created_at: :desc, id: :desc)
    .first

  return {} unless run

  run.workflow_steps.index_by(&:name).transform_values(&:status)
end

def dag_statuses_for(graph, workflow_class)
  step_statuses = current_step_statuses_for(workflow_class)

  graph.nodes.each_with_object({}) do |node, statuses|
    statuses[node.name] =
      step_statuses[node.name] ||
      step_statuses["#{node.name}_wait"] ||
      synthetic_child_start_status(step_statuses["#{node.name}_start"]) ||
      "defined"
  end
end

def synthetic_child_start_status(status)
  return unless status

  status == "succeeded" ? "running" : status
end

def dag_node_position(node)
  {
    "create_review_request" => [ 335, 48 ],
    "assess_risk" => [ 335, 166 ],
    "notify_reviewer" => [ 100, 300 ],
    "approval_decided" => [ 100, 418 ],
    "apply_manual_decision" => [ 100, 536 ],
    "auto_approve_request" => [ 570, 418 ],
    "capture_payment" => [ 335, 670 ],
    "release_order" => [ 145, 804 ],
    "cancel_request" => [ 525, 804 ],
  }[node.name]
end

def dag_node_label(type)
  case type.to_s
  when "wait_event"
    "wait event"
  when "workflow_call"
    "workflow call"
  else
    type.to_s.tr("_", " ")
  end
end

def render_dag_svg(graph, step_statuses)
  nodes = graph.nodes
  node_positions = {}
  width = 1_000
  node_width = 330
  node_height = 72
  left = (width - node_width) / 2
  top = 56
  row_gap = 118
  fallback_height = [ top * 2 + nodes.size * row_gap, 260 ].max

  nodes.each_with_index do |node, index|
    explicit_position = dag_node_position(node)
    node_positions[node.id] = {
      x: explicit_position&.first || left,
      y: explicit_position&.last || top + index * row_gap,
    }
  end

  height = [ fallback_height, node_positions.values.map { |position| position.fetch(:y) }.max.to_i + node_height + top ].max

  edges = graph.edges.filter_map do |edge|
    from = node_positions[edge.from]
    to = node_positions[edge.to]
    next unless from && to

    start_x = from.fetch(:x) + node_width / 2
    start_y = from.fetch(:y) + node_height
    end_x = to.fetch(:x) + node_width / 2
    end_y = to.fetch(:y)
    mid_y = start_y + ((end_y - start_y) / 2.0)

    <<~SVG
      <path class="dag-edge" d="M #{start_x} #{start_y} C #{start_x} #{mid_y}, #{end_x} #{mid_y}, #{end_x} #{end_y}" marker-end="url(#arrow)" />
      #{edge.condition ? %(<text class="dag-edge-label" x="#{end_x + 18}" y="#{mid_y - 4}">#{ERB::Util.html_escape(edge.condition)}</text>) : ""}
    SVG
  end.join

  node_markup = nodes.map do |node|
    position = node_positions.fetch(node.id)
    status = step_statuses.fetch(node.name, "defined")
    cls = status == "defined" ? "defined" : status_class(status)
    metadata = [
      dag_node_label(node.type),
      node.target_workflow_class,
    ].compact.join(" / ")

    <<~SVG
      <g class="dag-node" data-node="#{ERB::Util.html_escape(node.name)}" data-status="#{ERB::Util.html_escape(status)}" transform="translate(#{position.fetch(:x)} #{position.fetch(:y)})">
        <rect class="dag-card #{cls}" width="#{node_width}" height="#{node_height}" rx="8" />
        <text class="dag-node-name" x="18" y="27">#{ERB::Util.html_escape(node.name)}</text>
        <text class="dag-node-meta" x="18" y="51">#{ERB::Util.html_escape(metadata)}</text>
        <text class="dag-status-label" x="#{node_width - 18}" y="27" text-anchor="end">#{ERB::Util.html_escape(status)}</text>
      </g>
    SVG
  end.join

  <<~SVG
    <svg class="dag-svg" viewBox="0 0 #{width} #{height}" role="img" aria-label="DurableFlow definition DAG">
      <defs>
        <marker id="arrow" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto" markerUnits="strokeWidth">
          <path d="M0,0 L0,6 L9,3 z" fill="#8b94a3" />
        </marker>
      </defs>
      #{edges}
      #{node_markup}
    </svg>
  SVG
end

def render_dag_page
  workflow_label, workflow_class = DAG_WORKFLOWS.first
  graph = DurableFlow::DefinitionAnalyzer.call(workflow_class)
  step_statuses = dag_statuses_for(graph, workflow_class)
  warnings = graph.warnings

  <<~HTML
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>DurableFlow DAG Demo</title>
        <style>
          * { box-sizing: border-box; }
          body { margin: 0; background: #f5f6f8; color: #14171f; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; font-size: 14px; letter-spacing: 0; }
          .shell { min-height: 100vh; display: grid; grid-template-columns: 250px minmax(0, 1fr); }
          .side { background: #12151b; color: #f7f8fa; padding: 22px 18px; }
          .brand { display: flex; align-items: center; gap: 10px; margin-bottom: 28px; font-weight: 760; }
          .mark { width: 30px; height: 30px; border: 1px solid #343b46; border-radius: 8px; display: grid; place-items: center; color: #9df2bd; background: #1a1f26; }
          .side-link { display: block; color: #dce3ec; text-decoration: none; padding: 9px 10px; border-radius: 8px; margin-top: 4px; }
          .side-link.active { background: #202732; color: #fff; }
          .note { color: #a3adb9; line-height: 1.55; margin-top: 28px; }
          .page { max-width: 1220px; margin: 0 auto; padding: 30px 32px 44px; }
          .header { display: flex; justify-content: space-between; align-items: flex-start; gap: 18px; margin-bottom: 22px; }
          .kicker { color: #667085; font-size: 12px; font-weight: 760; text-transform: uppercase; margin-bottom: 6px; }
          h1 { margin: 0 0 7px; font-size: 30px; line-height: 1.15; letter-spacing: 0; }
          p { margin: 0; color: #667085; line-height: 1.5; }
          code { color: #dce3ec; background: #202732; padding: 2px 5px; border-radius: 5px; }
          button, a.button { min-height: 38px; border: 1px solid #cfd5de; background: #fff; color: #14171f; border-radius: 8px; padding: 0 12px; font-weight: 720; cursor: pointer; text-decoration: none; display: inline-flex; align-items: center; justify-content: center; }
          button.primary { background: #12151b; color: #fff; border-color: #12151b; }
          .actions { display: flex; gap: 10px; flex-wrap: wrap; justify-content: flex-end; }
          .grid { display: grid; grid-template-columns: minmax(0, 1fr) 360px; gap: 16px; align-items: start; }
          .panel { background: #fff; border: 1px solid #e1e5eb; border-radius: 8px; box-shadow: 0 1px 2px rgba(20,23,31,.06), 0 10px 26px rgba(20,23,31,.05); overflow: hidden; }
          .panel + .panel { margin-top: 16px; }
          .panel-head { min-height: 66px; padding: 15px 16px; border-bottom: 1px solid #e1e5eb; display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
          .title { font-weight: 780; }
          .subtitle { color: #667085; font-size: 12px; margin-top: 3px; overflow-wrap: anywhere; }
          .dag-wrap { padding: 10px 10px 18px; overflow: auto; }
          .dag-svg { display: block; width: 100%; min-width: 720px; height: auto; }
          .dag-edge { fill: none; stroke: #8b94a3; stroke-width: 2; }
          .dag-edge-label { fill: #667085; font-size: 12px; font-weight: 700; }
          .dag-card { fill: #fff; stroke: #cfd5de; stroke-width: 1.5; filter: drop-shadow(0 6px 10px rgba(20,23,31,.08)); transition: fill .16s ease, stroke .16s ease; }
          .dag-card.defined { fill: #fbfcfd; stroke: #cfd5de; }
          .dag-card.success { fill: #e7f6ef; stroke: #0c7f56; }
          .dag-card.waiting { fill: #fff2d8; stroke: #9b6100; }
          .dag-card.active { fill: #e8f1fb; stroke: #2767ad; }
          .dag-card.danger { fill: #fdecec; stroke: #b73535; }
          .dag-node-name { fill: #14171f; font-size: 15px; font-weight: 800; }
          .dag-node-meta { fill: #667085; font-size: 12px; }
          .dag-status-label { fill: #667085; font-size: 12px; font-weight: 760; }
          .side-section { padding: 15px 16px; border-bottom: 1px solid #e1e5eb; }
          .side-section:last-child { border-bottom: 0; }
          .stat { display: grid; grid-template-columns: 100px minmax(0, 1fr); gap: 10px; padding: 8px 0; border-bottom: 1px solid #e1e5eb; }
          .stat:last-child { border-bottom: 0; }
          .key { color: #667085; font-size: 12px; }
          .value { overflow-wrap: anywhere; }
          .warning { color: #9b6100; background: #fff2d8; padding: 10px 12px; border-radius: 8px; margin-top: 8px; }
          pre { margin: 0; padding: 14px; border: 0; background: #fbfcfd; overflow: auto; white-space: pre-wrap; font-size: 12px; line-height: 1.55; }
          @media (max-width: 900px) {
            .shell { grid-template-columns: 1fr; }
            .side { display: none; }
            .page { padding: 22px 16px 34px; }
            .header, .grid { display: block; }
            .actions { justify-content: flex-start; margin-top: 16px; }
            .right-column { margin-top: 16px; }
          }
        </style>
      </head>
      <body>
        <div class="shell">
          <aside class="side">
            <div class="brand"><div class="mark">D</div><div>DurableFlow<br><span style="color:#a3adb9;font-size:12px;font-weight:500">Live demo</span></div></div>
            <a class="side-link" href="/live">Live workflow</a>
            <a class="side-link active" href="/dag">Definition DAG</a>
            <a class="side-link" href="/durable_flow">Engine UI</a>
            <div class="note">This view renders analyzer output and colors nodes from live workflow step updates.</div>
          </aside>
          <main class="page">
            <div class="header">
              <div>
                <div class="kicker">Definition DAG</div>
                <h1>#{ERB::Util.html_escape(workflow_label)}</h1>
                <p>Static workflow structure from source analysis, overlaid with the latest run status from the live demo.</p>
              </div>
              <div class="actions">
                <button class="primary" data-post="/start">Start review run</button>
                <button data-post="/start_auto">Start auto-approved run</button>
                <a class="button" href="/live">Open timeline</a>
                <a class="button" href="/durable_flow">Open engine UI</a>
              </div>
            </div>

            <div class="grid">
              <section class="panel">
                <div class="panel-head">
                  <div>
                    <div class="title">Rendered Graph</div>
                    <div class="subtitle">#{ERB::Util.html_escape(workflow_class.name)} / #{graph.nodes.size} nodes / #{graph.edges.size} edges</div>
                  </div>
                </div>
                <div class="dag-wrap">
                  #{render_dag_svg(graph, step_statuses)}
                </div>
              </section>

              <div class="right-column">
                <aside class="panel">
                  <div class="panel-head"><div><div class="title">Graph Summary</div><div class="subtitle">Static analyzer output</div></div></div>
                  <div class="side-section">
                    <div class="stat"><div class="key">Workflow</div><div class="value">#{ERB::Util.html_escape(workflow_class.name)}</div></div>
                    <div class="stat"><div class="key">Source</div><div class="value">#{ERB::Util.html_escape(graph.source_file)}</div></div>
                    <div class="stat"><div class="key">Nodes</div><div class="value">#{graph.nodes.map { |node| ERB::Util.html_escape(node.name) }.join(", ")}</div></div>
                    #{warnings.map { |warning| %(<div class="warning">#{ERB::Util.html_escape(warning)}</div>) }.join}
                  </div>
                </aside>

                <aside class="panel">
                  <div class="panel-head"><div><div class="title">Graph JSON</div><div class="subtitle">DefinitionGraph#to_h</div></div></div>
                  <pre>#{ERB::Util.html_escape(JSON.pretty_generate(normalize_json(graph.to_h)))}</pre>
                </aside>
              </div>
            </div>
          </main>
        </div>

        <script>
          const primaryWorkflowClass = #{workflow_class.name.to_json};

          document.querySelectorAll("[data-post]").forEach((button) => {
            button.addEventListener("click", async () => {
              button.disabled = true;
              try {
                resetDagStatuses();
                await fetch(button.dataset.post, { method: "POST" });
              } finally {
                button.disabled = false;
              }
            });
          });

          function statusClass(status) {
            if (["completed", "succeeded", "matched"].includes(status)) return "success";
            if (["failed", "timed_out", "error"].includes(status)) return "danger";
            if (["waiting", "sleeping", "pending", "warn"].includes(status)) return "waiting";
            if (["running", "ready", "retrying"].includes(status)) return "active";
            return "defined";
          }

          function setNodeStatus(name, status) {
            document.querySelectorAll("[data-node]").forEach((node) => {
              if (node.dataset.node !== name) return;

              node.dataset.status = status;
              const card = node.querySelector(".dag-card");
              const label = node.querySelector(".dag-status-label");
              card.setAttribute("class", `dag-card ${statusClass(status)}`);
              label.textContent = status;
            });
          }

          function graphNodeNameForStep(stepName) {
            return stepName.replace(/_(start|wait)$/, "");
          }

          function graphStatusForStep(stepName, status) {
            if (stepName.endsWith("_start") && status === "succeeded") return "running";
            return status;
          }

          function resetDagStatuses() {
            document.querySelectorAll("[data-node]").forEach((node) => {
              setNodeStatus(node.dataset.node, "defined");
            });
          }

          const source = new EventSource("/events");
          source.addEventListener("change", (event) => {
            const change = JSON.parse(event.data);
            if (change.workflow_class && change.workflow_class !== primaryWorkflowClass) return;

            if (change.type === "workflow_run.created") resetDagStatuses();
            if (change.type.startsWith("workflow_step.")) {
              setNodeStatus(graphNodeNameForStep(change.name), graphStatusForStep(change.name, change.status));
            }
          });
        </script>
      </body>
    </html>
  HTML
end

def render_live_page
  run = current_run
  timeline = run&.timeline
  step_entries = timeline&.step_entries || []
  waits = timeline&.waits || []
  logs = timeline&.logs || []

  <<~HTML
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>DurableFlow Live Demo</title>
        <style>
          * { box-sizing: border-box; }
          body { margin: 0; background: #f5f6f8; color: #14171f; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; font-size: 14px; letter-spacing: 0; }
          .shell { min-height: 100vh; display: grid; grid-template-columns: 250px minmax(0, 1fr); }
          .side { background: #12151b; color: #f7f8fa; padding: 22px 18px; }
          .brand { display: flex; align-items: center; gap: 10px; margin-bottom: 28px; font-weight: 760; }
          .mark { width: 30px; height: 30px; border: 1px solid #343b46; border-radius: 8px; display: grid; place-items: center; color: #9df2bd; background: #1a1f26; }
          .side-link { display: block; color: #dce3ec; text-decoration: none; padding: 9px 10px; border-radius: 8px; margin-top: 4px; }
          .side-link.active { background: #202732; color: #fff; }
          .note { color: #a3adb9; line-height: 1.55; margin-top: 28px; }
          .page { max-width: 1220px; margin: 0 auto; padding: 30px 32px 44px; }
          .header { display: flex; justify-content: space-between; align-items: flex-start; gap: 18px; margin-bottom: 22px; }
          .kicker { color: #667085; font-size: 12px; font-weight: 760; text-transform: uppercase; margin-bottom: 6px; }
          h1 { margin: 0 0 7px; font-size: 30px; line-height: 1.15; letter-spacing: 0; }
          p { margin: 0; color: #667085; line-height: 1.5; }
          code { color: #dce3ec; background: #202732; padding: 2px 5px; border-radius: 5px; }
          .actions { display: flex; gap: 10px; flex-wrap: wrap; justify-content: flex-end; }
          button, a.button { min-height: 38px; border: 1px solid #cfd5de; background: #fff; color: #14171f; border-radius: 8px; padding: 0 12px; font-weight: 720; cursor: pointer; text-decoration: none; display: inline-flex; align-items: center; justify-content: center; }
          button.primary { background: #12151b; color: #fff; border-color: #12151b; }
          .grid { display: grid; grid-template-columns: minmax(0, 1.28fr) minmax(330px, .72fr); gap: 16px; align-items: start; }
          .panel { background: #fff; border: 1px solid #e1e5eb; border-radius: 8px; box-shadow: 0 1px 2px rgba(20,23,31,.06), 0 10px 26px rgba(20,23,31,.05); overflow: hidden; }
          .panel + .panel { margin-top: 16px; }
          .panel-head { min-height: 66px; padding: 15px 16px; border-bottom: 1px solid #e1e5eb; display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
          .title { font-weight: 780; }
          .subtitle { color: #667085; font-size: 12px; margin-top: 3px; overflow-wrap: anywhere; }
          .status { display: inline-flex; align-items: center; gap: 7px; min-height: 24px; padding: 0 9px; border-radius: 999px; font-size: 12px; font-weight: 760; white-space: nowrap; }
          .status:before { content: ""; width: 7px; height: 7px; border-radius: 50%; background: currentColor; }
          .success { color: #0c7f56; background: #e7f6ef; }
          .waiting { color: #9b6100; background: #fff2d8; }
          .active { color: #2767ad; background: #e8f1fb; }
          .danger { color: #b73535; background: #fdecec; }
          .timeline { padding: 6px 0; }
          .step { display: grid; grid-template-columns: 46px minmax(0, 1fr); padding: 0 16px; }
          .rail { position: relative; display: flex; justify-content: center; }
          .rail:after { content: ""; position: absolute; top: 32px; bottom: -8px; width: 1px; background: #e1e5eb; }
          .step:last-child .rail:after { display: none; }
          .dot { width: 18px; height: 18px; border-radius: 50%; margin-top: 18px; border: 3px solid #fff; box-shadow: 0 0 0 1px #cfd5de; background: #8b94a3; z-index: 1; }
          .dot.success { background: #0c7f56; } .dot.waiting { background: #9b6100; } .dot.active { background: #2767ad; } .dot.danger { background: #b73535; }
          .step-body { min-width: 0; padding: 13px 0 16px; border-bottom: 1px solid #e1e5eb; }
          .step:last-child .step-body { border-bottom: 0; }
          .step-top { display: flex; justify-content: space-between; align-items: center; gap: 12px; }
          .step-name { font-weight: 780; }
          .meta { color: #667085; font-size: 12px; margin-top: 5px; overflow-wrap: anywhere; }
          pre { margin: 12px 0 0; padding: 12px; border: 1px solid #e1e5eb; border-radius: 8px; background: #fbfcfd; overflow: auto; white-space: pre-wrap; font-size: 12px; line-height: 1.55; }
          .side-section { padding: 15px 16px; border-bottom: 1px solid #e1e5eb; }
          .side-section:last-child { border-bottom: 0; }
          .kv { display: grid; grid-template-columns: 88px minmax(0, 1fr); gap: 10px; padding: 8px 0; border-bottom: 1px solid #e1e5eb; }
          .kv:last-child { border-bottom: 0; }
          .key { color: #667085; font-size: 12px; }
          .value { overflow-wrap: anywhere; }
          .live-log { max-height: 260px; overflow: auto; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }
          .live-log-row { padding: 8px 0; border-bottom: 1px solid #e1e5eb; overflow-wrap: anywhere; }
          .step-logs { margin-top: 12px; border: 1px solid #e1e5eb; border-radius: 8px; background: #fbfcfd; overflow: hidden; }
          .step-logs-label { padding: 9px 12px; border-bottom: 1px solid #e1e5eb; color: #667085; font-size: 11px; font-weight: 760; text-transform: uppercase; }
          .step-log { padding: 11px 12px; border-bottom: 1px solid #e1e5eb; }
          .step-log:last-child { border-bottom: 0; }
          .step-log-top { display: flex; align-items: center; gap: 9px; flex-wrap: wrap; }
          .step-log-message { font-weight: 700; }
          .empty { padding: 30px 16px; color: #667085; text-align: center; }
          .empty.compact { padding: 10px 0; text-align: left; }
          @media (max-width: 900px) {
            .shell { grid-template-columns: 1fr; }
            .side { display: none; }
            .page { padding: 22px 16px 34px; }
            .header, .grid { display: block; }
            .actions { justify-content: flex-start; margin-top: 16px; }
            .right-column { margin-top: 16px; }
          }
        </style>
      </head>
      <body>
        <div class="shell">
          <aside class="side">
            <div class="brand"><div class="mark">D</div><div>DurableFlow<br><span style="color:#a3adb9;font-size:12px;font-weight:500">Live demo</span></div></div>
            <a class="side-link active" href="/live">Live workflow</a>
            <a class="side-link" href="/dag">Definition DAG</a>
            <a class="side-link" href="/durable_flow">Engine UI</a>
            <div class="note">This page listens to <code>DurableFlow.live_broadcaster</code> over EventSource and updates when workflow rows commit.</div>
          </aside>
          <main class="page">
            <div class="header">
              <div>
                <div class="kicker">Live lifecycle stream</div>
                <h1>Risk-Gated Review Workflow</h1>
                <p>Start a high-risk run to exercise the human branch, or an auto-approved run to exercise the low-risk path. Timeline rows, waits, and explicit workflow logs update without a refresh.</p>
              </div>
              <div class="actions">
                <button class="primary" data-post="/start">Start review run</button>
                <button data-post="/start_auto">Start auto-approved run</button>
                <button data-post="/approve">Approve</button>
                <button data-post="/reject">Reject</button>
                <button data-post="/reset">Reset demo</button>
                <a class="button" href="/durable_flow">Open engine UI</a>
              </div>
            </div>

            <div class="grid">
              <section class="panel">
                <div class="panel-head">
                  <div>
                    <div class="title">Timeline</div>
                    <div class="subtitle" id="run-subtitle">#{run ? "#{ERB::Util.html_escape(run.workflow_class)} / #{ERB::Util.html_escape(run.run_id)}" : "No run yet"}</div>
                  </div>
                  <span id="run-status" class="status #{run ? status_class(run.status) : "active"}">#{run&.status || "idle"}</span>
                </div>
                <div class="timeline" id="timeline">
                  #{render_steps(step_entries)}
                </div>
              </section>

              <div class="right-column">
                <aside class="panel">
                  <div class="panel-head"><div><div class="title">Run State</div><div class="subtitle">Database-backed workflow state</div></div></div>
                  <div class="side-section">
                    <div class="kv"><div class="key">Run</div><div class="value" id="run-id">#{run&.run_id || "-"}</div></div>
                    <div class="kv"><div class="key">Waits</div><div class="value" id="waits">#{ERB::Util.html_escape(wait_summary(waits))}</div></div>
                  </div>
                </aside>

                <aside class="panel">
                  <div class="panel-head"><div><div class="title">Live Events</div><div class="subtitle">Committed model changes</div></div></div>
                  <div class="side-section"><div class="live-log" id="live-log"></div></div>
                </aside>
              </div>
            </div>
          </main>
        </div>

        <script>
          const timeline = document.getElementById("timeline");
          const liveLog = document.getElementById("live-log");
          const runStatus = document.getElementById("run-status");
          const runSubtitle = document.getElementById("run-subtitle");
          const runId = document.getElementById("run-id");
          const waits = document.getElementById("waits");
          const primaryWorkflowClass = #{LiveReviewWorkflow.name.to_json};
          const steps = new Map();
          const workflowLogs = new Map();

          #{step_entries.map { |entry| "steps.set(#{entry.id.to_json}, #{step_to_json(entry.step).to_json});" }.join("\n")}
          #{logs.map { |log| "workflowLogs.set(#{log.id.to_json}, #{log_to_json(log).to_json});" }.join("\n")}

          renderTimeline();

          document.querySelectorAll("[data-post]").forEach((button) => {
            button.addEventListener("click", async () => {
              button.disabled = true;
              try {
                if (button.dataset.post === "/start" || button.dataset.post === "/reset") {
                  steps.clear();
                  workflowLogs.clear();
                  liveLog.innerHTML = "";
                  runId.textContent = "-";
                  waits.textContent = "-";
                  runSubtitle.textContent = button.dataset.post === "/reset" ? "No run yet" : runSubtitle.textContent;
                  if (button.dataset.post === "/reset") {
                    runStatus.textContent = "idle";
                    runStatus.className = "status active";
                  }
                  renderTimeline();
                }
                await fetch(button.dataset.post, { method: "POST" });
              } finally {
                button.disabled = false;
              }
            });
          });

          function statusClass(status) {
            if (["completed", "succeeded", "matched"].includes(status)) return "success";
            if (["failed", "timed_out", "error"].includes(status)) return "danger";
            if (["waiting", "sleeping", "pending", "warn"].includes(status)) return "waiting";
            return "active";
          }

          function escapeHtml(value) {
            return String(value).replace(/[&<>"']/g, (char) => ({
              "&": "&amp;",
              "<": "&lt;",
              ">": "&gt;",
              '"': "&quot;",
              "'": "&#039;"
            }[char]));
          }

          function renderBlock(value) {
            if (!value || (typeof value === "object" && Object.keys(value).length === 0)) return "";
            return `<pre>${escapeHtml(JSON.stringify(value, null, 2))}</pre>`;
          }

          function renderTimeline() {
            if (steps.size === 0) {
              timeline.innerHTML = '<div class="empty">No steps yet. Start a run.</div>';
              return;
            }

            timeline.innerHTML = [...steps.values()].sort((a, b) => a.id - b.id).map((step) => {
              const cls = statusClass(step.status);
              const details = step.display_result || step.display_metadata;
              return `<article class="step"><div class="rail"><span class="dot ${cls}"></span></div><div class="step-body"><div class="step-top"><div><div class="step-name">${escapeHtml(step.name)}</div><div class="meta">Attempt ${step.attempts || 0}</div></div><span class="status ${cls}">${escapeHtml(step.status)}</span></div>${renderBlock(details)}${renderStepLogs(step)}</div></article>`;
            }).join("");
          }

          function renderStepLogs(step) {
            const logs = [...workflowLogs.values()]
              .filter((log) => log.workflow_step_id === step.id)
              .sort((a, b) => a.id - b.id);

            if (logs.length === 0) return "";

            return `<div class="step-logs"><div class="step-logs-label">Logs</div>${logs.map((log) => {
              const cls = statusClass(log.level);
              return `<div class="step-log"><div class="step-log-top"><span class="status ${cls}">${escapeHtml(log.level)}</span><span class="step-log-message">${escapeHtml(log.message)}</span></div>${renderBlock(log.display_data)}</div>`;
            }).join("")}</div>`;
          }

          function appendLiveEvent(change) {
            const row = document.createElement("div");
            row.className = "live-log-row";
            row.textContent = `${new Date().toLocaleTimeString()} ${change.type}`;
            liveLog.prepend(row);
          }

          const source = new EventSource("/events");
          source.addEventListener("change", (event) => {
            const change = JSON.parse(event.data);
            if (change.workflow_class && change.workflow_class !== primaryWorkflowClass) return;

            appendLiveEvent(change);

            if (change.type === "workflow_run.created") {
              steps.clear();
              workflowLogs.clear();
              renderTimeline();
            }

            if (change.run_id) runId.textContent = change.run_id;
            if (change.workflow_class && change.run_id) runSubtitle.textContent = `${change.workflow_class} / ${change.run_id}`;

            if (change.type.startsWith("workflow_run.")) {
              runStatus.textContent = change.status;
              runStatus.className = `status ${statusClass(change.status)}`;
            }

            if (change.type.startsWith("workflow_step.")) {
              steps.set(change.id, change);
              renderTimeline();
            }

            if (change.type.startsWith("workflow_wait.")) {
              waits.textContent = `${change.event_name}: ${change.status}`;
            }

            if (change.type === "workflow_log.created") {
              workflowLogs.set(change.id, change);
              renderTimeline();
            }
          });
        </script>
      </body>
    </html>
  HTML
end

def http_response(socket, body, status: "200 OK", headers: {})
  default_headers = {
    "Content-Type" => "text/html; charset=utf-8",
    "Content-Length" => body.bytesize,
    "Connection" => "close",
  }

  socket.write "HTTP/1.1 #{status}\r\n"
  default_headers.merge(headers).each { |key, value| socket.write "#{key}: #{value}\r\n" }
  socket.write "\r\n"
  socket.write body
end

def no_content_response(socket)
  http_response(socket, "", status: "204 No Content")
end

def handle_events(socket)
  queue = Queue.new
  CLIENTS_MUTEX.synchronize { CLIENTS << queue }

  socket.write "HTTP/1.1 200 OK\r\n"
  socket.write "Content-Type: text/event-stream\r\n"
  socket.write "Cache-Control: no-cache\r\n"
  socket.write "Connection: keep-alive\r\n\r\n"
  socket.write ": connected\n\n"

  loop do
    payload = queue.pop
    socket.write "event: change\n"
    socket.write "data: #{JSON.generate(payload)}\n\n"
    socket.flush
  end
ensure
  CLIENTS_MUTEX.synchronize { CLIENTS.delete(queue) } if queue
end

def handle_rails_app(socket, method, full_path)
  env = Rack::MockRequest.env_for("http://127.0.0.1:4568#{full_path}", method: method)
  status, headers, body = DurableFlowLiveDemoApp.call(env)
  chunks = []
  body.each { |chunk| chunks << chunk }
  body.close if body.respond_to?(:close)
  response_body = chunks.join

  socket.write "HTTP/1.1 #{status}\r\n"
  headers.each { |key, value| socket.write "#{key}: #{value}\r\n" }
  socket.write "Content-Length: #{response_body.bytesize}\r\nConnection: close\r\n\r\n"
  socket.write response_body
end

def read_headers(socket)
  loop do
    line = socket.gets
    break if line.nil? || line == "\r\n"
  end
end

port = Integer(ENV.fetch("PORT", 4568))
server = TCPServer.new("127.0.0.1", port)
puts "DurableFlow live demo: http://127.0.0.1:#{port}/live"

loop do
  socket = server.accept

  Thread.new(socket) do |client|
    ActiveRecord::Base.connection_pool.with_connection do
      request_line = client.gets
      next unless request_line

      method, full_path = request_line.split
      path = full_path.to_s.split("?").first
      read_headers(client)

      case [ method, path ]
      when [ "GET", "/" ], [ "GET", "/live" ]
        http_response(client, render_live_page)
      when [ "GET", "/dag" ]
        http_response(client, render_dag_page)
      when [ "GET", "/events" ]
        handle_events(client)
      when [ "POST", "/start" ]
        start_demo_run!
        no_content_response(client)
      when [ "POST", "/start_auto" ]
        start_demo_run!(amount_cents: 42_000)
        no_content_response(client)
      when [ "POST", "/approve" ]
        complete_demo_run!("approved")
        no_content_response(client)
      when [ "POST", "/reject" ]
        complete_demo_run!("rejected")
        no_content_response(client)
      when [ "POST", "/reset" ]
        reset_demo!
        no_content_response(client)
      else
        handle_rails_app(client, method, full_path)
      end
    end
  rescue StandardError => error
    warn "#{error.class}: #{error.message}"
  ensure
    client&.close unless client&.closed?
  end
end
