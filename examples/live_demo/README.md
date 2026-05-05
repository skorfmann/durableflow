# DurableFlow Live Demo

This is a small Rails application embedded in one Ruby file. It mounts the
DurableFlow engine, stores workflow state in SQLite, and streams committed
workflow changes to the browser with Server-Sent Events.

Run it from the repository root:

```sh
mise exec ruby@3.4 -- bundle exec ruby examples/live_demo/server.rb
```

Then open:

```text
http://127.0.0.1:4568/live
```

The demo workflow creates a review request, invokes a child risk assessment
workflow, branches between human review and auto-approval, and then either
invokes a child payment-capture workflow or cancels the request. The page
updates live from `DurableFlow.live_broadcaster` and also links to the mounted
engine UI at `/durable_flow`. Start can create multiple runs so the engine UI
shows run history; use **Reset demo** to clear the local demo database.

The demo also renders a static definition DAG from `DurableFlow::DefinitionAnalyzer`
and colors nodes from the same live event stream:

```text
http://127.0.0.1:4568/dag
```

The mounted engine exposes the generic run-scoped version for every workflow run:

```text
http://127.0.0.1:4568/durable_flow/workflow_runs/:run_id/definition
```

The local SQLite database is written to `examples/live_demo/db/live.sqlite3` and
is ignored by git.
