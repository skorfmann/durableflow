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

class LiveReviewWorkflow < DurableFlow::Workflow
  def perform(review_id)
    request = step(:create_review_request) do
      log.info("Creating review request", review_id: review_id, amount_cents: 124_000)

      {
        id: review_id,
        amount_cents: 124_000,
        customer: "Northwind",
      }
    end

    step(:notify_reviewer) do
      log.info("Waiting for human decision", review_id: request.fetch(:id), channel: "demo-ui")
      true
    end

    decision = step.wait_for_event(:approval_decided, match: { review_id: request.fetch(:id) })

    step(:apply_decision) do
      log.warn(
        "Applying human decision",
        review_id: request.fetch(:id),
        decision: decision.fetch(:decision),
        decided_by: decision.fetch(:decided_by),
      )

      {
        review_id: request.fetch(:id),
        decision: decision.fetch(:decision),
        decided_by: decision.fetch(:decided_by),
      }
    end
  end
end

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
  DurableFlow::WorkflowRun.order(created_at: :desc, id: :desc).first
end

def start_demo_run!
  clear_demo_data!
  clear_test_jobs!
  LiveReviewWorkflow.perform_now("review-#{SecureRandom.hex(3)}")
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

  ActiveJob::Base.deserialize(run.reload.serialized_job).perform_now
  clear_test_jobs!
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

def render_steps(steps, logs)
  return '<div class="empty">No steps yet. Start a run.</div>' if steps.empty?

  logs_by_step_id = logs.select(&:workflow_step_id).group_by(&:workflow_step_id)

  steps.map do |step|
    payload = safe_value { step.result_value } || step.metadata_hash.presence
    cls = status_class(step.status)
    step_logs = logs_by_step_id.fetch(step.id, [])

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
          #{render_step_logs(step_logs)}
        </div>
      </article>
    HTML
  end.join
end

def render_live_page
  run = current_run
  steps = run ? run.workflow_steps.order(:created_at, :id).to_a : []
  waits = run ? run.workflow_waits.order(:created_at, :id).to_a : []
  logs = run ? run.workflow_logs.includes(:workflow_step).ordered.to_a : []

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
            <a class="side-link" href="/durable_flow">Engine UI</a>
            <div class="note">This page listens to <code>DurableFlow.live_broadcaster</code> over EventSource and updates when workflow rows commit.</div>
          </aside>
          <main class="page">
            <div class="header">
              <div>
                <div class="kicker">Live lifecycle stream</div>
                <h1>Human Review Workflow</h1>
                <p>Start a run, then approve or reject it. Timeline rows, waits, and explicit workflow logs update without a refresh.</p>
              </div>
              <div class="actions">
                <button class="primary" data-post="/start">Start run</button>
                <button data-post="/approve">Approve</button>
                <button data-post="/reject">Reject</button>
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
                  #{render_steps(steps, logs)}
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
          const steps = new Map();
          const workflowLogs = new Map();

          #{steps.map { |step| "steps.set(#{step.id.to_json}, #{step_to_json(step).to_json});" }.join("\n")}
          #{logs.map { |log| "workflowLogs.set(#{log.id.to_json}, #{log_to_json(log).to_json});" }.join("\n")}

          renderTimeline();

          document.querySelectorAll("[data-post]").forEach((button) => {
            button.addEventListener("click", async () => {
              button.disabled = true;
              try {
                if (button.dataset.post === "/start") {
                  steps.clear();
                  workflowLogs.clear();
                  liveLog.innerHTML = "";
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
      when [ "GET", "/events" ]
        handle_events(client)
      when [ "POST", "/start" ]
        start_demo_run!
        no_content_response(client)
      when [ "POST", "/approve" ]
        complete_demo_run!("approved")
        no_content_response(client)
      when [ "POST", "/reject" ]
        complete_demo_run!("rejected")
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
