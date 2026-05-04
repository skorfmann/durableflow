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

The demo workflow creates a review request, waits for a human approval event,
then applies the decision. The page updates live from `DurableFlow.live_broadcaster`
and also links to the mounted engine UI at `/durable_flow`.

The local SQLite database is written to `examples/live_demo/db/live.sqlite3` and
is ignored by git.
