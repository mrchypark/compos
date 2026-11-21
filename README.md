# Compos

Compos is a Work OS Kernel: users model work with objects, relationships,
state machines, and automations, and every business change is executed as a
validated Kernel command.

This feature branch uses this runtime split:

- Go owns the API server, Kernel command runtime, embedded SQL runtime, and search syncer.
- `github.com/mrchypark/rhiza` is the embedded canonical SQL/Graph/KV runtime;
  `github.com/ncruces/go-sqlite3` is the local/test storage driver.
  Compos pins Rhiza v0.27.3, which provides SQL, Graph, KV,
  atomic statement batches, and bounded native graph traversal in that same
  embedded runtime. Local SQLite mode uses WAL, immediate write transactions, per-connection
  foreign keys, a busy timeout, RFC3339 timestamps, schema identity checks, and
  online backups. Rhiza mode owns commits, checkpoints, and recovery.

The legacy PocketBase-backed application implementation has been removed from
this greenfield tree. Accounts and workspace-to-person identity mapping belong
outside Compos. Compos issues no sessions and implements no authorization policy.
New product work must follow the ADRs and specs under `docs/`.

The product's highest-level vocabulary and boundaries are defined by the
[Compos Constitutional Architecture](docs/architecture/constitutional-architecture.md).

## Non-Negotiables

- The Kernel is the only canonical writer.
- A user action, API call, or automation is a command intent until the Kernel
  validates and commits it.
- Automations may emit command intents; they must not write database rows.
- `object_instances` are the canonical storage model for business
  objects.

## Target Layout

- `cmd/server`: Go API server.
- `cmd/search-syncer`: Meilisearch projection worker.
- `internal/kernel`: Go Kernel command runtime.
- `internal/store`: canonical DDL and local SQLite connection policy.
- `docs/adr`: binding architectural decisions.
- `docs/specs`: runtime and interface contracts.

## Current Implementation Notes

- `COMPOS_RHIZA_DATA_DIR=./data go run ./cmd/server` starts Compos with an
  in-process Rhiza SQL node. No standalone database server is started.
  `COMPOS_SQLITE_PATH` selects the database and `COMPOS_BIND` selects the bind
  address. In local SQLite mode,
  `go run ./cmd/server -backup /safe/path/backup.sqlite` takes an online backup
  and exits; direct SQLite backup is disabled in Rhiza mode.
  When object storage is configured, durability defaults to `before-ack`;
  multi-node membership refuses to start with a weaker mode. Rhiza-mode Kernel
  commands and workspace provisioning commit as deterministic atomic batches.
- `COMPOS_CONDUCTOR_URL` enables delivery of connector/native Function requests
  from the durable outbox to Conductor. `COMPOS_CONDUCTOR_TOKEN` authenticates
  outbound delivery. Incoming callback and connector requests are accepted by
  the authless server and must be network-restricted by the deployment gateway.
  The server stores callback evidence,
  then uses an internal idempotent Kernel command to append the terminal
  Function result and evaluate Task completion.
- The public executable registry is `internal/kernel/registry.go` and contains
  generic object, relation, automation, and deletion commands.
- SQLite is a declared local/test single-node support mode. Embedded Rhiza owns
  production canonical commits, checkpoint bootstrap, replicated logs, and
  recovery. A single Rhiza node with remote `before-ack` durability can recover
  from object storage, but does not provide multi-node HA.
- Compos has no authentication or authorization mode. HTTP routes accept requests
  without credentials and attribute every request to the fixed principal `local`.
  Protected deployments must enforce access control and authenticate callers at
  the trusted network boundary before traffic reaches Compos. Compos does not
  receive or propagate caller identity. Kernel actors are attribution metadata.
  Workspace creation records `local` as the creator principal for attribution
  and starts with an empty domain model.
  There is no dev fallback, local account/session API, membership table, or
  capability management endpoint, and no principal-person auth binding.
- See `docs/adr/0002-go-sqlite-runtime.md` for schema, JSON, concurrency,
  relation-schema, indexing, time, automatic-number, and backup decisions.
- Knowledge content and its lifecycle belong to an external system. Compos retains
  immutable Ontology revisions through `spec.publish` and Task `ontology_context`.
  Domain Event instances use schema-validated `event.emit`; Functions validate
  and emit pinned Event revisions. Composite Functions execute pinned Workflows.
  Object search uses the external Meilisearch projection populated by
  `cmd/search-syncer`; the server enforces the workspace filter on every request.
  Restarting the syncer rebuilds the active workspace from an object-only snapshot,
  clearing obsolete search documents before indexing current objects.
- Fresh databases use 22 application tables and schema identity
  `compos-go-sqlite-authless-v7`. Old identities are rejected; there is
  no automatic auth compatibility migration or live-data conversion.
  Search removes hits without a valid `object_id` in the server, so existing
  indexes work without a syncer restart or new filter settings. Responses report
  `returnedHits` for the filtered page and omit engine totals and facets.

Object-type naming is a breaking contract: every definition must provide a
plural slug in `key` plus explicit `singular` and `plural` names. There is no
separate slug field, alias, fallback, or inflection.
Object type and object instance IDs remain the stable persisted identifiers and
are never derived from display names.

## Docker

Build the runtime images:

```bash
docker build --target server -t compos-server:local .
docker build --target search-syncer -t compos-search-syncer:local .
```

Run the supported single-node server on `127.0.0.1:7878` with a Docker volume:

```bash
docker volume create compos-data
docker run --rm \
  -p 127.0.0.1:7878:7878 \
  -e COMPOS_BIND=0.0.0.0:7878 \
  -e COMPOS_SQLITE_PATH=/data/compos.sqlite \
  -v compos-data:/data \
  compos-server:local
```

This starts the standalone SQLite runtime. Put `/data` on durable storage and
schedule verified backups outside the node. The server does not perform user
authentication; place it behind the deployment's trusted gateway if access control
is required.

The `search-syncer` image consumes the server's object projection and writes
the Meilisearch index. See
[the storage and deployment contract](docs/reverse-spec/05-storage-deployment.md)
and [the API/search contract](docs/reverse-spec/06-api-search.md) for their
required environment and data prerequisites.

For local CRUD with the same credential-free default, run the server directly:

```bash
go run ./cmd/server \
  -bind 127.0.0.1:3000 \
  -database ./compos.sqlite
```

Requests then use `http://127.0.0.1:3000` without credentials and are audited as
the fixed principal `local`. The SQLite path is standalone only. Set `-rhiza-data` for a single-node
Rhiza durable runtime; supply more than one `COMPOS_RHIZA_MEMBERS` entry and a
`before-ack` object store for HA.

## License

Compos is licensed under the [Apache License 2.0](LICENSE). See the
[third-party notices](THIRD_PARTY_NOTICES.md) and
[third-party license texts](THIRD_PARTY_LICENSES.txt) for dependencies and
remaining notice review.
