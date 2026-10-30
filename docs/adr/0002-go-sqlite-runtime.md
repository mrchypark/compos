# ADR 0002: Go SQLite Runtime

## Status

Superseded for HA deployments by [ADR 0003](0003-embedded-rhiza-sql-ha.md).
SQLite-only mode remains accepted for local development and tests.

## Decision

The API server and Kernel run in Go and use `github.com/ncruces/go-sqlite3`
through `database/sql`. When explicitly selected for local development or
tests, SQLite is that process's single-node canonical store, not a test
harness. It is not the deployed Rhiza authority. The Rust Kubernetes operator
does not turn SQLite into a replicated database.

The Go runtime supports Rauthy and PocketBase upstream verification and issues
opaque hashed Compos sessions. Session lookup derives the principal and logout
revokes the presented session. Explicit local `none` mode uses the fixed `dev`
principal and does not trust a caller-supplied principal header; bind that mode
to loopback or a trusted private boundary.

Every connection enables `foreign_keys`, WAL, a busy timeout, RFC3339 time
encoding, and immediate write transactions. The pool defaults to eight
connections. Startup runs `PRAGMA optimize=0x10002`, the long-running server
runs `PRAGMA optimize` daily, and shutdown runs it once more so recursive
relation queries do not depend on missing or stale planner statistics. Writes
that change an aggregate also write the command ledger,
activity, outbox, and projection event in the same transaction. Deployments
must use a durable local filesystem, one active server process per database,
and online backups copied to a different failure domain.

`schema_metadata` contains one exact schema identity, not a migration version.
Startup creates only an empty database and rejects a legacy, partial, or foreign
database. Schema changes are breaking: recreate the database explicitly rather
than incrementing a version or applying an implicit migration.

All JSON columns have `json_valid()` constraints and object/array constraints
where their shape is fixed. Field schemas are recursively checked when an
object type is registered; unsupported keywords and invalid nested constraints
are rejected. Object values are recursively validated on create and replace.

`relation_definitions` is the only editable relationship definition. Each
object type's `relation_schema_json` is a deterministic derived snapshot of
the active normalized rows. Defining or archiving a relationship publishes a
new immutable schema revision for each affected object type. Revision digests
cover `schema_version`, `field_schema`, `relation_schema`, and
`state_machine_id`.

Automatic string fields are generated only inside the object-create
transaction. IANA time zones and RFC3339 instants determine date reset scopes;
`field_sequence_counters` allocates the next value under the immediate write
lock, and `generated_field_values` reserves the final value with a unique key.
Clients cannot set or replace generated fields.

`object_field_index` stores top-level scalar values in typed columns and backs
string, number, and boolean indexes. Local search uses that table plus the
object title. Nested structured values remain canonical JSON and are not
indexed until a concrete query requirement justifies path-specific rows.

All persisted instants are UTC RFC3339 with nanoseconds. Unix-second strings
are not accepted as a second storage convention.

Online backups use the driver's SQLite backup API. A backup is useful only
after it is copied away from the database host and periodically restored in a
verification job. Command-log repair and confirmed replay APIs are not part of
the Go server surface.
