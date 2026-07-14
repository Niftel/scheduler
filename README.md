# scheduler

Praetor's **scheduler** service — the control-plane brain that decides what runs
and turns launches into durable work.

Its responsibilities:

- **schedules & triggers** — evaluates rrule schedules, event triggers, and inbound
  webhooks, launching templates and workflows when due (`triggers.go`, `workflow.go`),
- **the outbox relay** — publishes queued launches onto the [`eventbus`](https://github.com/praetordev/eventbus)
  with at-least-once delivery, so a launch committed to the DB is never lost
  (`publisher.go`, `relay_integration_test.go`),
- **stale-run detection** — flags runs whose executor died into `reconciling` for the
  reconciler to resolve (`scheduler.go`),
- **manifests, pruning, notifications, and Galaxy credential resolution**.

It is a leaf deployable: nothing imports it in production. It depends only on the
shared `praetordev/*` libraries (`eventbus`, `events`, `models`, `store`, `db`,
`launch`, `objectstore`, `notify`, `crypto`, `metrics`, `env`, `plog`).

## Layout

```
main.go     entrypoint
core/       scheduler loop, triggers, workflow, outbox relay/publisher,
            manifest, pruner, notify, galaxy, metrics
```

## Build the image

```
docker build -t praetor-scheduler:latest .
```

Stable image name (`praetor-scheduler`) so the Helm chart and k3d/kind load step
are unaffected by the repo split.

## Tests

The DB/NATS-backed integration tests are gated on `TEST_DATABASE_URL` /
`TEST_NATS_URL` and skip without them.

```
go test ./...
```
