# Rust-to-Go Runtime Cutover

Status: Go runtime cutover guidance; current Rhiza feature-branch acceptance
and deployment evidence are tracked separately in the implementation issue ledger.

The application server, CLI, Kernel, canonical schema, Workflow coordinator,
ReactiveRule evaluator, authentication, and search projection feed are Go. Rust
remains only for the Kubernetes operator. There is no Rust application runtime
to keep in dual-write or compatibility mode.

## Storage boundary

The Go schema is intentionally fresh-only and rejects a database whose exact
`schema_metadata.identity` differs. Do not point the Go server at a Rust,
PocketBase, older Go, partial, or foreign database. No generic migration engine
is provided.

For a deployment with existing business data:

1. Stop old writers and retain a recoverable backup of the old store.
2. Export only supported business definitions and instances from the old
   runtime using its documented export surface.
3. Start the Go server with an empty durable store and the required Rauthy or
   PocketBase, Conductor, Meilisearch, and Rhiza configuration.
4. Recreate definitions first, then replay vetted business records through
   registered Kernel commands. Never insert canonical rows directly.
5. Start `cmd/search-syncer`; let its snapshot plus projection-event cursor
   rebuild Meilisearch before enabling search traffic.
6. Verify health, authentication/session/logout, one Human submission, one
   external Function callback, one Workflow completion, one ReactiveRule
   action, one Agent result submission, object search, and Rhiza
   reachability.
7. Keep the old runtime read-only until record counts, sampled payloads,
   relations, command receipts, and search results are accepted, then retire it.

An automated importer should be added only when a concrete legacy export format
and a real cutover dataset exist. Without that source contract, an importer
would invent mappings and risk silently changing authority or history.
