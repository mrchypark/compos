# Compos

Compos is a Work OS Kernel: users model work with objects, relationships,
state machines, and automations, and every business change is executed as a
validated Kernel command.

This feature branch uses this runtime split:

- Go owns the CLI, API server, Kernel command runtime, and embedded SQL runtime.
- Rust owns the Kubernetes operator.
- `github.com/mrchypark/rhiza` is the embedded canonical SQL/Graph/KV runtime;
  `github.com/ncruces/go-sqlite3` is the local/test storage driver.
  Compos pins Rhiza v0.19.0, which provides SQL, Graph, KV,
  atomic statement batches, and bounded native graph traversal in that same
  embedded runtime. Local SQLite mode uses WAL, immediate write transactions, per-connection
  foreign keys, a busy timeout, RFC3339 timestamps, schema identity checks, and
  online backups. Rhiza mode owns commits, checkpoints, and recovery.
- The Go CLI reads canonical state from the Go server. It does not keep a
  local business-state read replica.
- The Go CLI keeps command-intent and receipt evidence in an embedded bbolt
  request ledger before any Kernel submission. `ledger list` shows request
  metadata; `ledger retry` recovers unsent/unknown submissions against the pinned
  server and user. No S2 service or SDK is required.
- The Rust operator renders fixed-membership Rhiza StatefulSets with private
  UDP peer networking, `emptyDir` local materialization, and shared object-store
  checkpoints. Its multi-node rollout remains an integration gate, not a live
  production claim.

The legacy PocketBase-backed application implementation has been removed from
this greenfield tree. Accounts/authentication belong to Goauthy and authorization
to Grant; Compos does not issue sessions or store memberships/capabilities.
New product work must follow the ADRs and specs under `docs/`.

The product's highest-level vocabulary and boundaries are defined by the
[Compos Constitutional Architecture](docs/architecture/constitutional-architecture.md).
Its staged implementation is tracked in the
[Constitutional Architecture Implementation Plan](docs/plans/constitutional-architecture-implementation.md).

## Non-Negotiables

- The Kernel is the only canonical writer.
- A user action, API call, or automation is a command intent until the Kernel
  validates and commits it.
- The first durable record of any CLI write intent is the local request ledger.
  The CLI must not bypass the journal with a direct write submit to the Kernel.
- Automations may emit command intents; they must not write database rows.
- `object_instances` are the canonical storage model for business
  objects.
- Local CLI request ledgers are evidence, not authority.

## Target Layout

- `cmd/compos`: Go CLI.
- `cmd/server`: Go API server.
- `internal/cli`: Go CLI local request ledger.
- `internal/kernel`: Go Kernel command runtime.
- `internal/store`: canonical DDL and local SQLite connection policy.
- `crates/compos-operator`: Rust Kubernetes operator.
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
  outbound delivery and `COMPOS_CONDUCTOR_CALLBACK_TOKEN` protects
  `POST /internal/v1/conductor/callbacks`. The server stores callback evidence,
  then uses an internal idempotent Kernel command to append the terminal
  Function result and evaluate Task completion. The same service token protects
  the read-only Compos connector schema/record endpoints; set Conductor's
  `COMPOS_SERVICE_TOKEN` to the same value. These handler token checks are
  additional to external Authority approval; they do not make service authorization
  operational without a separately approved service contract and adapter.
- The public executable registry is `internal/kernel/registry.go` and contains
  generic object, relation, automation, and deletion commands. Legacy
  `task.*`, `task_submission.*`, `compos task`, and `compos submission`
  commands do not exist.
- SQLite is a declared local/test single-node support mode. Embedded Rhiza owns
  production canonical commits, checkpoint bootstrap, replicated logs, and
  recovery. The tg900 deployment is one Rhiza node with `emptyDir` plus GCS
  `before-ack` durability: it verifies recovery durability, not multi-node HA.
- Authentication and authorization enter through `server.NewWithAuthority` and
  Kernel `AuthorizeCommand`. No Goauthy/Grant network adapter is implemented.
  `server.New` (also used by the current server entrypoint) denies all requests
  except GET health until a trusted authority is supplied. There is no dev fallback,
  local account/session API, membership table, or capability management endpoint.
  See [the external authority contract](docs/reverse-spec/03-auth-permissions.md).
- See `docs/adr/0002-go-sqlite-runtime.md` for schema, JSON, concurrency,
  relation-schema, indexing, time, automatic-number, and backup decisions.
- Local `setup/login/logout/whoami` account commands have been removed.
  CLI identity/credentials must come from an injected external integration; the
  default CLI has no operational network adapter. Mutating commands use
  `COMPOS_LEDGER_PATH` or the platform data-dir default; they do not
  start a local database sidecar.
- Knowledge content and its lifecycle belong to an external system. Compos retains
  immutable Ontology revisions through `spec.publish` and Task `ontology_context`.
  Domain Event instances use schema-validated `event.emit`; Functions validate
  and emit pinned Event revisions. Composite Functions execute pinned Workflows.
  Object search uses the external Meilisearch projection populated by
  `cmd/search-syncer`; the server enforces the workspace filter on every request.
  Restarting the syncer rebuilds the active workspace from an object-only snapshot,
  clearing obsolete search documents before indexing current objects.
- Fresh databases use 22 application tables and schema identity
  `compos-go-sqlite-external-authority-v5`. Old identities are rejected; there is
  no automatic auth compatibility migration or live-data conversion.
  Search removes hits without a valid `object_id` in the server, so existing
  indexes work without a syncer restart or new filter settings. Responses report
  `returnedHits` for the filtered page and omit engine totals and facets.

## CLI Object Type Words

The CLI consumes externally supplied identity and credentials in memory. It does
not read `$HOME/.compos/auth.json`, implement local login/logout, or fall back to
a fixed dev user. Per-command `--token` flags remain removed.
The built-in dynamic `people` object type (singular `person`) is business data,
not a local account or authorization registry.
Every workspace also has a
reserved `companies` object type whose singular name is `company`, a
deterministic own workspace company object, and a reserved `works_for` relation
definition from `people` to `companies`.
There is no `employment` object, table, or Kernel aggregate in the current
model. Principal-person auth bindings have been removed; ordinary business
relations do not grant authorization. Person and company instances
otherwise behave like normal objects: `object.update` can change the object
`title` and replace its `values` payload. Replacement values must retain every
field required by the latest object type schema; a values replacement repins the
object to that latest schema, while a title-only update retains its current pin.
Object identity, type, workspace, and
creation metadata are system fields outside the payload and are immutable. Use `relation.link` and `relation.unlink` for ordinary
person-to-company or person-to-business relationships, including `works_for`.

`compos workspace create --server URL --name NAME [--key KEY]` is a control-plane helper,
not a Kernel command. The CLI posts `POST /api/v1/workspaces` with externally
supplied credentials and an idempotency key; a trusted authority must approve it. When `--key` is supplied, the CLI and
server trim it and ASCII-lowercase it before deterministic hashing and
provisioning. The workspace provisioning path creates the workspace, creator
person, deterministic workspace company, and creator `works_for` relation
atomically and idempotently in
canonical SQL. The response `created_at` value is UTC RFC3339.

Object-type naming is a breaking contract: every definition must provide a
plural slug in `key` plus explicit `singular` and `plural` names. There is no
separate slug field, alias, fallback, or inflection. Pre-existing user-defined
object types are not migrated. Definitions are created with plural keys such as
`contracts`; the reserved built-in keys are `people` and `companies`. The
plural key is the operational CLI command group.
Object type and object instance IDs remain the stable persisted identifiers and
are never derived from display names.

After defining an object type key, the CLI can use that plural key as an
operational noun. The noun is resolved through the workspace command surface
before the CLI prints help or submits anything, so typos and undefined words
fail before audit or Kernel command submission.

These aliases still build normal Kernel command intents; they do not create a
separate product-specific API surface. CLI writes append the intent to the
embedded local request ledger first, then submit to the Compos server command
endpoint.
`contracts create` is a CLI noun form for `object.create` with
`payload.object_type_key = "contracts"`.

```bash
compos workspace create --server http://127.0.0.1:3000 --name "Acme Research" --key acme-research
compos web --server http://127.0.0.1:3000 --workspace WORKSPACE_ID
compos define --server http://127.0.0.1:3000 --workspace WORKSPACE_ID \
  --key contracts --singular contract --plural contracts \
  --field-schema '{"properties":{}}'
compos help contracts
compos contracts help
compos contracts create --server http://127.0.0.1:3000 --workspace WORKSPACE_ID --title "Acme Contract"
compos object update --id obj_1 --version 1 --title "Acme Contract v2"
compos contracts move --id obj_1 --version 1 --to-state review
compos contracts link --definition rdf_1 --definition-version 1 --from obj_1 --from-version 1 --to obj_2 --to-version 1
```

`compos web` starts a local server-side-rendered console on `127.0.0.1:8787` by
default and opens the browser. It consumes the injected external credentials
through the local Go process, keeps bearer credentials out of the browser, and is read-only
by default. `compos web --enable-writes` enables the narrow object create/edit
form surface; submissions retain CSRF/origin checks and become normal audited
Kernel commands rather than browser-side API writes. Object views consume the
workspace HTTP command surface and render native schema-derived forms. Persisted
HTML templates and their selection/submission contract have been removed.
Backoffice owns custom layouts and widgets; Compos owns data contracts.
On edit, the console fetches the current full values,
overlays the configured submitted fields, and submits a version-preconditioned
full replacement. This intentionally preserves hidden, structured, and
currently unsupported values while retaining compare-and-set protection.

## Docker

Build the server image:

```bash
docker build --target server -t compos-server:local .
```

Build the CLI image, including the embedded web console:

```bash
docker build --target cli -t compos-cli:local .
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

This starts the SQLite process and health endpoint; it does not configure a
Goauthy/Grant authority adapter. Other HTTP operations remain denied. Put `/data`
on durable storage and schedule verified backups outside the node. Provider
service/resource contracts and the network adapter remain integration work.
