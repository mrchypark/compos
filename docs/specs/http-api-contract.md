# HTTP API Contract

The Go server exposes a small command-first HTTP API. Business mutations go
through the Kernel command endpoint only.

The current Work read projection is:

```text
GET /api/v1/workspaces/{workspace_id}/work
GET /api/v1/workspaces/{workspace_id}/work/{stream_id}
```

The collection route lists open `work.routed` facts for all human participants,
including their actual participant IDs, excluding terminal work and a
route after its matching submission. Workflow routes carry the semantic
`workflow_step_id` and invocation-scoped `workflow_route_id`; parallel or repeated
Workflow invocations remain separate inbox entries and `work.submit` must preserve
both returned identities. A non-Workflow submission omits both IDs and resolves
to the latest unresolved route for that actor. Re-routing after a
submission opens the work for the new Human assignment. Submission remains the
existing audited `work.submit` Kernel command; there is no inbox-specific
mutation route.

It returns the pinned Task revision and spec, owner, context, projected status,
current assignee, submissions, Function/Agent/Workflow executions, active invocation
ids, timestamps, stream version, and immutable Journal facts. Status is replayed
as `open`, `assigned`, `running`, `awaiting_review`, `completed`, or `cancelled`;
it is never stored as mutable TaskRun state. All writes still use the Kernel
command endpoint.

Task revisions published with `spec.publish` require `name`, `goal`, object
`input_schema`, object `output_schema`, and `completion`. Supported completion
contracts are `object_state_equals` and `submission_valid`. Optional
`constraints`, `capability_policy.allowed_function_revision_ids`, and
`ontology_context` are pinned in the same immutable revision. `work.open`
validates `context` against the input contract, `work.submit` validates the
submission against the pinned route's optional `output_schema` (otherwise the
Task output contract), and `function.invoke` enforces a
non-empty capability allowlist whenever `capability_policy` is present. An
omitted policy is unrestricted; an explicitly empty policy is rejected.

`work.open.deadline_at` is optional RFC3339. The scheduler appends exactly one
`work.deadline.reached` fact while the Work is open; notification or escalation
is expressed as a ReactiveRule triggered by that fact. A Task `failure_policy`
may route to a Human or cancel after final `function.failed` or `agent.failed`.
There are no generic timer/message/wait nodes.

`SpecRevision(kind=participant)` stores an AgentDefinition. Routing to its pinned
revision appends `agent.requested` and a Conductor outbox request. A successful
callback appends `agent.succeeded` and `work.submitted`; a valid
`submission_valid` result also satisfies ordinary Work. Human and Agent workflow
routes remain open until the submission carrying the same `workflow_step_id` and
`workflow_route_id`;
parallel route submissions converge independently and the enclosing Workflow
performs final Work completion. Function nodes remain open until
`function.succeeded` or `function.failed`. Publishing a Function or Workflow
rejects any reachable composite Function↔Workflow execution cycle.

## Definition and relation reads

```text
GET /api/v1/workspaces/{workspace_id}/definitions
GET /api/v1/workspaces/{workspace_id}/definitions/{revision_id}
GET /api/v1/workspaces/{workspace_id}/objects/{object_id}/relations
```

Definition reads expose immutable `task`, `function`, and `workflow` revisions.
The collection accepts `kind`, `family_id`, `limit` (1–100), and `offset`;
`page.next_cursor` is the next offset. Editing publishes a new revision with
`spec.publish`; existing Work stays pinned. Relation reads accept `direction`
(`incoming`, `outgoing`, default `both`), `relation_definition_id`, `limit`, and
`offset`. They return actual edge IDs/versions, endpoint IDs, and related record
summaries. These workspace reads are canonical local reads, not
Meilisearch or Graph projections.

The following binding and submission examples assume a configured workspace
with the referenced object types, schemas, relations, and Task definitions.
New workspaces start empty; define object types with `object_type.define` and
define any required relations before creating objects or publishing bindings.

Task `data_bindings` is an optional list of at most 32 semantic record bindings.
UI layout, widgets and form drafts remain client-owned. Each binding has:

```json
{
  "id": "record",
  "object_type_id": "<record-type-id>",
  "schema_version": 2,
  "context_key": "object_id",
  "writable_fields": ["description"],
  "allow_title": true,
  "allowed_operations": ["object.update"]
}
```

`context_key` must be a required string in Task `input_schema` and resolves from
the immutable Work context. An optional `relation_path` traverses up to four
canonical edges; each hop declares `relation_definition_id`, its `version`, and
`direction` (`incoming` or `outgoing`). The terminal type must match the binding.
Publication validates type/schema/field/path references. Submission checks the
current pins again; schema evolution requires a newly published Task and new Work
for edits under the new contract, not silent migration of existing Work.

`work.open` validates every binding context record in its workspace: the record
must be active and match the root type of the pinned relation path, including
context keys other than `object_id`. Invalid roots are rejected before opening
an immutable Work stream.

Route steps may declare `output_schema`, `output_mapping`, and a nonempty
`data_binding_ids` list narrowing the Task bindings. Omitted binding IDs inherit
the Task binding scope. The Kernel resolves the contract from the immutable
Workflow revision and invocation-scoped route ID, never a client-supplied schema.
Human and Agent submitted values populate `steps.<step_id>` and `current`;
route `output_mapping` follows the same selector rules as invoke steps. A human
handoff continues under the submitter recorded in the Journal. Work participant,
Function binding and pinned object checks remain domain preconditions.

`work.submit` without `changes` still records a submission only. Optional
`changes` is a nonempty list of at most 32 bounded operations:

```json
{
  "submission": {"decision": "approved"},
  "workflow_step_id": "review",
  "workflow_route_id": "<route-id-from-inbox>",
  "changes": [{
    "binding_id": "record",
    "command_type": "object.update",
    "target": {"aggregate_type": "object_instance", "aggregate_id": "<record-id>"},
    "preconditions": [{"aggregate_type": "object_instance", "aggregate_id": "<record-id>", "expected_version": 1}],
    "payload": {"values": {"description": "Reviewed"}}
  }]
}
```

Only `object.create`, `object.update`, `relation.link`, and `relation.unlink` are
accepted, and only when explicitly allowed by the pinned binding. The ordinary
command payload/precondition rules still apply. Replacement `values` cannot
remove or change non-writable fields; generated fields are omitted and preserved
by the existing object planner. `title` requires `allow_title`.

All changes, the submission, one parent command receipt, and each affected
aggregate's activity/outbox/projection event commit together in one SQLite or
Rhiza transaction. A failed validation, stale version, terminal Work or invalid
assignee commits nothing. Replaying the same parent idempotency key does not
repeat child mutations. The receipt's projection sequence covers the last event;
new records can be read through their canonical relations after submission.

There is one mutation per existing aggregate. New child records use
`object.create.relations`; the binding path ends with an incoming hop and the
create carries that relation. Its parent may be an existing record or another
creation in the same submission. Arbitrary command batches are not supported.
There are no external calls inside the transaction.

The 32-change input bound is not a promise that every combination fits storage:
Rhiza v0.12 also limits a transaction to 64 SQL statements, including guards,
field indexes, journal and projection facts. Identical pre-submit guards are
deduplicated and exact row-count assertions are combined. A plan exceeding that native limit returns `validation_failed`
before any write; the server never splits an atomic submission into smaller
commits. Larger forms require a storage-capacity change or a different business
transaction boundary, not client-side sequential saves disguised as one submit.

### Submission-local references and automatic numbers

Each new `object.create` may carry a unique `client_ref` (a bounded Workflow-style
identifier). A reference is the singleton JSON object `{"client_ref":"parent"}`;
plain strings remain existing server IDs. References are accepted in create
`relations`, `relation.link` endpoint fields and the `submission` value. They are
resolved only within this request; unknown, duplicate, malformed and cyclic
creation references fail before writing. Input order does not determine creation
order. All IDs are assigned using the parent command identity and original change
index before validation; the server orders dependent creates and preserves the
original index in errors. New link endpoints have server-known version 1; existing
endpoint/definition preconditions still follow the ordinary command contract.

For example, an existing root record is pinned in Work `context.root_id`. The
`parent` binding traverses incoming `parent.root`; the `item` binding traverses
incoming `parent.root` then incoming `item.parent`. Both permit `object.create`
and `allow_title`. Schemas supply generated number fields, which are omitted from
the request. A payload fragment is:

```json
{
  "submission": {"parent_id": {"client_ref": "p"}},
  "changes": [
    {"binding_id":"item","client_ref":"i1","command_type":"object.create","target":{"aggregate_type":"object_instance"},"payload":{"object_type_key":"items","title":"Item 1","values":{},"relations":{"parent":{"client_ref":"p"}}}},
    {"binding_id":"parent","client_ref":"p","command_type":"object.create","target":{"aggregate_type":"object_instance"},"payload":{"object_type_key":"parents","title":"Parent","values":{},"relations":{"root":"<existing-root-id>"}}},
    {"binding_id":"item","client_ref":"i2","command_type":"object.create","target":{"aggregate_type":"object_instance"},"payload":{"object_type_key":"items","title":"Item 2","values":{},"relations":{"parent":{"client_ref":"p"}}}},
    {"binding_id":"item","client_ref":"i3","command_type":"object.create","target":{"aggregate_type":"object_instance"},"payload":{"object_type_key":"items","title":"Item 3","values":{},"relations":{"parent":{"client_ref":"p"}}}}
  ]
}
```

Compos reserves numbers per workspace/type/field/reset scope using the existing
generation template. Rhiza writes the final counter once with a compare-and-set
guard; SQLite advances counters inside the same transaction. Generated values
may refer to an earlier dependency's generated field. Failed saves roll back
counters with records, relations, receipts and submission facts. Concurrent
reservations cannot reuse a number: a losing optimistic commit can be retried
with the unchanged command ID, idempotency key and payload. A successful retry
returns the persisted mapping rather than allocating again.

The success receipt adds `client_refs`, persisted in the parent command ledger:

```json
{"client_refs":{"p":{"id":"<assigned-id>","version":1,"generated_values":{"number":"P-001"}},"i1":{"id":"<assigned-id>","version":1,"generated_values":{"number":"I-001"}}}}
```

The Journal contains the resolved submission, so the next Workflow step receives
actual IDs. Clients replace draft IDs/numbers only after a successful receipt.
On an uncertain network result it retries the exact same command. On rejection it
keeps all drafts. Per-change validation errors add `error.details` with
`change_index` (zero-based original order), `client_ref`, `binding_id`, `field`
when known, and `code`. Transaction-level capacity, concurrency and storage errors
do not invent a row or field; they apply to the whole submission. Unknown storage
failures are safe, retryable `quorum_unavailable` responses, not validation errors.

Internal Function `kernel_command` bindings allow `object.move` and
`object.update` only. Both require the pinned Work `context.object_id` and a
positive `expected_version`; update input declares `title` and/or `values`.
The normal object validation and Function output schema apply in the same commit.

## Versioning

Versioned API endpoints are under:

```text
/api/v1
```

Breaking request/response changes require a new API version.

## Authentication

Compos has no authentication, session, or authorization mode. Every registered
HTTP route accepts credential-free requests and records the fixed principal
`local`. Access restriction and caller authentication belong to the deployment
network boundary. No `/api/v1/auth/*`, membership, or capability endpoints exist.

## Idempotency keys

Clients must generate a unique idempotency key for each intended operation and
reuse that key when retrying the same operation. Because every caller uses the
`local` principal, workspace creation keys share one namespace across all
callers, and Kernel command keys share a namespace across all callers within
each workspace. Workspace creation and Kernel commands use separate namespaces.

## Workspace Create

```text
POST /api/v1/workspaces
Content-Type: application/json
```

This is a control-plane provisioning endpoint, not a Kernel command and not an
entry in `GET /api/v1/kernel/command-registry`.

The server records fixed principal `local` as `creator_principal_id`. The HTTP
API requires `idempotency_key`. When `key` is provided, the server trims and
ASCII-lowercases it before deterministic request hashing and storage.

Request:

```json
{
  "name": "Acme Research",
  "key": "acme-research",
  "idempotency_key": "idem_workspace_acme"
}
```

`key` is optional. Workspace creation atomically provisions:

- the `workspaces` row;
- the idempotency/provisioning record.

The new workspace has no domain object types, objects, or relation definitions.
Before creating an object, define its type with `object_type.define`; see the
[Kernel command contract](kernel-command-contract.md#payload-schema)
for the definition payload.

Repeating the same request with the same scoped idempotency key returns the
stored result with duplicate status. Reusing the idempotency key with different
workspace parameters fails with `409 idempotency_conflict`; a duplicate key
fails with `409 workspace_key_conflict`.

Success response is `201 Created` for a new workspace and `200 OK` for an
idempotent duplicate:

```json
{
  "data": {
    "id": "wks_...",
    "workspace_id": "wks_...",
    "key": "acme-research",
    "name": "Acme Research",
    "version": 1,
    "creator_principal_id": "local",
    "created_at": "2026-09-01T00:00:00Z",
    "created": true,
    "status": "created"
  }
}
```

`created_at` is the stored RFC 3339 timestamp. The route is implemented for
both local SQLite and embedded Rhiza storage.

## Kernel Command Execution

```text
POST /api/v1/kernel/commands
Content-Type: application/json
```

This endpoint is the Kernel execution boundary for API callers. Embedded Rhiza
owns HA replication and recovery; the server does not expose command-log repair
or confirmed-replay endpoints.

The Go SQLite support mode accepts an 8 MiB JSON command envelope. The example
assumes `contracts` has already been defined in this workspace.

Request:

```json
{
  "command_id": "018f9c5e-...",
  "workspace_id": "wks_...",
  "idempotency_key": "client-generated-key",
  "requested_actor": {
    "type": "user",
    "id": "usr_..."
  },
  "command_type": "object.create",
  "target": {
    "aggregate_type": "object_instance",
    "aggregate_id": null
  },
  "preconditions": [],
  "payload": {
    "object_type_key": "contracts",
    "title": "Acme Contract",
    "values": {}
  },
  "causation": {
    "source": "api",
    "attempt_id": null,
    "event_id": null
  }
}
```

## Update Object

`object.update` updates an object instance. The example assumes a configured
person type whose schema includes `display_name` and `email`. The
command targets the object id and requires exactly one matching
`object_instance` precondition with the current version. Its payload contains
`title`, `values`, or both:

```json
{
  "command_type": "object.update",
  "target": {
    "aggregate_type": "object_instance",
    "aggregate_id": "obj_..."
  },
  "preconditions": [
    {
      "aggregate_type": "object_instance",
      "aggregate_id": "obj_...",
      "expected_version": 1
    }
  ],
  "payload": {
    "title": "Ada Lovelace",
    "values": {
      "display_name": "Ada Lovelace",
      "email": "ada@example.com"
    }
  }
}
```

For a person, `title` is the editable object title/name and fields under
`values` are the editable person payload. `values` is a full replacement, not
a merge or patch, so it must satisfy the latest immutable schema revision and
updates the instance pin to that revision. Clients cannot select a revision:
`schema_version` and `schemaVersion` are rejected as unknown fields. Omitting
`title` preserves the current title; a title-only update preserves both current
values and their existing schema pin.

Object id, object type, workspace, and creation metadata are immutable system
fields outside the payload. State is also outside this command and changes only
through `object.move`.

## Evolve Object Type Schema

`object_type.schema_evolve` publishes a complete immutable field-schema snapshot.
It targets one active `object_type` and requires that type's current
aggregate version as its CAS precondition. The payload is:

```json
{
  "field_schema": {
    "type": "object",
    "properties": {
      "display_name": { "type": "string" }
    },
    "required": ["display_name"],
    "additionalProperties": false
  }
}
```

Any supported complete field schema is allowed. Compos does not migrate existing
object payloads. The new revision inherits the previous relation schema and state
machine definition, increments `schema_version`, and stores a new digest. A payload
may omit `field_schema.state_machine`, in which case Compos copies it from the
previous immutable revision. If the payload includes that member, it must exactly
match the previous definition; attempts to add, remove, or change it are rejected.
Existing instances retain their pins until a values update validates against and
pins the latest revision.

### Storage baseline

Fresh databases carry the canonical
`compos-go-sqlite-world-spec-journal-reactive-agent-v4` identity. Existing v3
databases migrate atomically on open by dropping only `ontology_groundings`,
`knowledge_relations` and `knowledge_documents`, including their contents, then
publishing v4. Shared specs and history remain. Other identities are rejected;
this is not mixed-version writer compatibility.

## Command Recovery Boundary

The server has no S2 command-log, repair, manual quorum-control, or
confirmed-replay HTTP surface. An ambiguous API submission is retried with the
original command ID, idempotency key, principal and workspace. Invalid or
mismatched receipts remain outcome-unknown; only validated command receipts
confirm commitment. Embedded Rhiza owns cluster checkpoint bootstrap,
replicated logs, and recovery.

## Error Shape

All non-2xx responses use this envelope:

```json
{
  "error": {
    "code": "precondition_failed",
    "category": "conflict",
    "retryable": false,
    "message": "object version changed",
    "fields": [
      {
        "path": "preconditions[0].expected_version",
        "code": "stale",
        "message": "expected version 4 but found 5"
      }
    ]
  }
}
```

HTTP mapping:

- `400 validation_failed`
- `403 forbidden`
- `404 not_found`
- `409 precondition_failed`
- `409 idempotency_conflict`
- `409 workspace_key_conflict`
- `409 snapshot_required`
- `422 schema_mismatch`
- `503 quorum_unavailable`

## Read Object

```text
GET /api/v1/workspaces/{workspace_id}/objects/{object_id}
```

Response from an example workspace where `contracts` has been defined:

```json
{
  "data": {
    "id": "obj_...",
    "workspace_id": "wks_...",
    "object_type_key": "contracts",
    "title": "Acme Contract",
    "values": {},
    "state_key": "draft",
    "version": 1,
    "schema_version": 1,
    "schema_digest": "sha256:..."
  },
  "consistency": {
    "mode": "canonical_linearizable",
    "projection_sequence": 42,
    "source": "kernel"
  }
}
```

## List Objects

Object values are returned exactly as stored. Each object includes its pinned
`schema_version` and the matching immutable `schema_digest`; reads never validate
or transform historical payloads.

## Schema Revisions

```text
GET /api/v1/workspaces/{workspace_id}/object-types/{object_type_id}/schema-revisions?limit=50&before=4
```

Workspace members can retrieve every immutable schema revision through bounded
pages. `limit` defaults to 50 and must be from 1 through 100. `before` is an
optional exclusive `schema_version` cursor. Results are ordered by descending
`schema_version`; `page.next_cursor` is the next `before` value or `null` when
the history is exhausted. Pagination never deletes or compacts revisions. Each
entry contains the complete field/relation/state snapshot, digest, and creation
provenance.

```text
GET /api/v1/workspaces/{workspace_id}/objects?object_type=contracts
```

The first implementation may support only object type filtering and cursor
pagination.

Response:

```json
{
  "data": [],
  "page": {
    "next_cursor": null
  },
  "consistency": {
    "mode": "canonical_stale_bounded",
    "projection_sequence": 42,
    "source": "kernel"
  }
}
```

## Projection Sync

```text
GET /api/v1/workspaces/{workspace_id}/projection/events?after=41&limit=100
```

Response:

```json
{
  "events": [
    {
      "id": "prj_...",
      "workspace_id": "wks_...",
      "command_id": "cmd_...",
      "projection_sequence": 42,
      "event_type": "object.updated",
      "aggregate_type": "object_instance",
      "aggregate_id": "obj_...",
      "aggregate_version": 1,
      "payload_hash": "sha256:...",
      "payload": {},
      "created_at": "2026-06-08T00:00:00Z"
    }
  ]
}
```

Object projection events contain the complete search document fields. The
SearchSyncer upserts `object.created`, `object.updated`, and `object.moved`
including the canonical aggregate version, and
deletes only on `object.deleted`; event type is taken from the outer canonical
event, never inferred from payload shape.
`limit` defaults to 100 and is bounded to 1–1000.

## Projection Snapshot

```text
GET /api/v1/workspaces/{workspace_id}/projection/snapshot
```

Response:

```json
{
  "snapshot": {
    "snapshot_id": "snap_wks_1_42",
    "workspace_id": "wks_...",
    "projection_sequence": 42,
    "created_at": "2026-06-08T00:00:00Z",
    "signature": null,
    "signing_key_id": null,
    "schema_version": 3,
    "data_hash": "sha256:...",
    "objects": []
  }
}
```

The snapshot is a full projection baseline for the supported projection schema.
Schema version 3 includes only object projections. `data_hash` is
the canonical hash of the snapshot data object:

```json
{
  "objects": []
}
```

The current snapshot is unsigned; `signature` and `signing_key_id` are `null`.

## Domain Events and external Knowledge

`GET /api/v1/workspaces/{workspace_id}/events` reads Journal-derived Domain Events.
Mutations use `event.emit` through the Kernel command endpoint. Knowledge is
externally owned; Knowledge, Concept traversal, reachability, grounding and
proposal endpoints have been removed. Generic Ontology specs remain supported.

If the requested `after` sequence is too old, the server returns:

```json
{
  "error": {
    "code": "snapshot_required",
    "category": "resync",
    "retryable": false,
    "message": "projection log compacted"
  }
}
```

## Health

```text
GET /api/v1/health
```

Response:

```json
{
  "status": "ok",
  "storage": {
    "kind": "sqlite",
    "cluster_id": "sqlite-local",
    "ready": true
  }
}
```

Rhiza readiness failures return `503 storage_unavailable`; the server does not
synthesize a quorum claim from local process or Kubernetes state.

## Command Registry

```text
GET /api/v1/kernel/command-registry
```

Response:

```json
{
  "commands": [
    {
      "command_type": "object.create",
      "wire_name": "object.create",
      "payload_schema": {},
      "allowed_target_aggregate_types": ["object_instance"],
      "target_aggregate_id": "must_be_null",
      "required_preconditions": [],
      "authorization_policy": "...",
      "idempotency_behavior": "...",
      "emitted_events": ["object.created"]
    }
  ]
}
```

This is a response-shape example only. `GET /api/v1/kernel/command-registry`
serializes `Registry()` from `internal/kernel/registry.go`
and is authoritative for the registered command list, schemas, policies,
preconditions, action allowlist flags, and emitted events. See
`command-registry.md` for reader-facing registry notes. The endpoint is
read-only registry metadata. It is not workspace-specific and does not expose
generic mutation capabilities.

## Workspace Command Surface

```text
GET /api/v1/workspaces/{workspace_id}/command-surface
```

Returns definition-backed object nouns and command metadata for a workspace.

Response from an example workspace with the shown types and relation configured:

```json
{
  "workspace_id": "wks_1",
  "nouns": [
    {
      "key": "contracts",
      "singular": "contract",
      "plural": "contracts",
      "kind": "object_type",
      "object_type_id": "otp_contract",
      "version": 1,
      "commands": ["create", "get", "move", "link"],
      "field_schema": {},
      "state_machine_id": null,
      "relations": [
        {
          "id": "rdf_project_contracts",
          "key": "project_contracts",
          "version": 1,
          "from_object_type_id": "otp_project",
          "to_object_type_id": "otp_contract",
          "cardinality": "one_to_many"
        }
      ]
    }
  ]
}
```

This noun shape is a breaking contract. `key` is the required plural slug and
the only dynamic dispatch key; `singular` supplies one-object prose and
`plural` labels the collection group. All three are emitted explicitly. There
is no separate `slug`, singular-key alias, fallback, or inflection. No pre-existing user-defined object types are migrated
by this contract.

`object_type_id` is the stable canonical identity. Noun strings do not replace
or regenerate stable object type, object instance, relation definition, or
relation IDs.

Presentation templates are removed in a breaking change: nouns no longer expose
`templates`, and `object_type.template_set` is not a registered command. The v3
fresh schema has no template table. Existing v2 databases are rejected rather
than silently modified; retain a backup and perform a separately planned logical
data migration into a fresh database (and fresh Rhiza cluster identity/storage).
No automatic migration or deletion of existing database files is performed.
Clients own screen layouts and widgets; Compos retains schemas, semantic
Task bindings and atomic submission validation.

## Adapter Inbound Events

```text
POST /api/v1/workspaces/{workspace_id}/adapter/inbound-events
```

Request:

```json
{
  "adapter_id": "slack",
  "inbound_event_id": "evt_external_1",
  "payload": {}
}
```

Response:

```json
{
  "data": {
    "id": "ain_...",
    "workspace_id": "wks_...",
    "adapter_id": "slack",
    "inbound_event_id": "evt_external_1",
    "status": "appended",
    "received_at": "..."
  }
}
```

This endpoint appends to the inbound adapter event ledger only. It must not
mutate object, relation, command ledger, activity, outbox, or
projection rows. Kernel-owned reducers may later read inbound events and emit
command intents through the Kernel command execution path.
`adapter_id` and `inbound_event_id` are trimmed and must each contain 1 to 128
bytes. Replaying the same IDs and canonical payload returns the original row;
reusing them with different content returns `409 idempotency_conflict`.

### Credential pins

Connector Function bindings and Agent `execution_policy` may pin `connection_id`
and `credential_revision`. A supplied revision must use positive JSON
integer-literal syntax (not decimal or exponent notation), be no greater than
`9007199254740991`, and require a nonempty connection ID. Both are copied from
the immutable definition
into the request Journal and `_compos` outbox metadata on SQLite and Rhiza;
dispatch retry retains that snapshot. The revision means logical authorization
generation, not an internal OAuth token refresh/CAS version.

These pins do not prove profile ownership, consent or permission.

### Conductor dispatch acknowledgment

Compos sends durable outbox events to Conductor `POST /v1/events`. A successful
HTTP response alone does not mark them published: the body must be one JSON
document of at most 1 MiB, without trailing data, and must
contain `accepted: true` and exactly one matching acknowledgment per requested
`execution_request_id`, with a nonempty `execution_id`. Duplicate, missing or
foreign acknowledgments are rejected. An optional `event_id` must match.
Rejected acknowledgments leave the outbox pending for an idempotent retry.
Accepted execution states are `queued`, `running`, `retry_wait`, `succeeded`,
`failed`, `timed_out` and `cancelled`; replay can acknowledge an already terminal
execution. Dispatch acknowledgment is not a terminal Work result.

### Conductor terminal callback

```text
POST /internal/v1/conductor/callbacks
```

The callback must include `callback_id`, `workspace_id`,
`execution_request_id`, `execution_id`, `provider_key`, `provider_run_ref`,
`handler_version`, a positive `attempt_count`, RFC3339 `occurred_at`, and a
terminal `runtime_status`. The server verifies
that the workspace contains a matching external `function.requested` Journal fact
and pinned Conductor outbox request, and requires `provider_key` and
`handler_version` to equal that request's pinned execution identity,
appends an `adapter_inbound_events` row with `adapter_id=conductor`, and invokes
the internal idempotent `function.record_result` or `agent.record_result` Kernel
reducer. The reducer appends the matching terminal Function/Agent fact and evaluates the Task
completion contract; it does not write World tables directly. Terminal fact and
inbound `reduced` status commit atomically. A background reducer retries durable
`appended` callbacks after process interruption.
Replaying the same callback id and content returns success; reusing the id for
different content returns `409 idempotency_conflict`. A different callback ID
with the same canonical status/output/error digest converges to the existing
result even when Kernel output or event validation normalized it to failure;
a conflicting terminal result returns `409 terminal_result_conflict`. A replay
of the already reduced callback ID remains idempotent, but any new callback ID
arriving after the Work is satisfied or cancelled is rejected and cannot append
Function, Agent, submission, or completion facts.

### Conductor Compos connector reads

```text
GET /v1/connector/objects/{object_key}/schema
GET /v1/connector/objects/{object_key}/records/{record_id}
X-Conductor-Workspace-Id: <workspace_id>
```

These endpoints are the narrow service boundary for Conductor's built-in
`provider=compos` connector. Workspace scope is mandatory; access restriction
belongs to the deployment network boundary.
Mutation and arbitrary operation endpoints are intentionally absent; Compos
business writes remain Kernel commands.

## Search

```text
POST /api/v1/workspaces/{workspace_id}/search
Content-Type: application/json
```

Full-text search proxied through Meilisearch. The server enforces the workspace
filter. Requires `COMPOS_MEILISEARCH_URL` server
configuration; returns `503 search_unavailable` when Meilisearch is not
configured.

Request:

```json
{
  "q": "contract",
  "filter": "type = \"contract\"",
  "sort": ["updated_at:desc"],
  "limit": 20,
  "offset": 0,
  "attributesToRetrieve": ["id", "title", "type", "state"]
}
```

All fields are optional; an empty `q` performs a filtered listing. `filter` accepts Meilisearch filter syntax.
`sort` is an array of sortable attributes with optional direction suffix.
The proxy sends the mandatory workspace predicate and caller filter as separate
top-level AND terms, so caller syntax cannot widen the workspace boundary.
The server retrieves `object_id` internally even for restricted attribute requests,
discards hits without a non-empty string object ID, and hides that internal field
unless requested. Existing indexes need no new filterable attributes. Engine
totals and facets are omitted because they can include legacy documents;
`returnedHits` counts only returned objects on this page. Offsets retain engine
semantics, so pages may be short or empty before a workspace snapshot rebuild.

Success response:

```json
{
  "data": {
    "hits": [
      {
        "id": "obj_...",
        "workspace_id": "wks_...",
        "type": "contract",
        "title": "Acme Contract",
        "state": "active",
        "version": 1,
        "created_at": "2026-06-08T00:00:00Z",
        "updated_at": "2026-06-08T00:00:00Z"
      }
    ],
    "returnedHits": 1,
    "limit": 20,
    "offset": 0,
    "processingTimeMs": 1
  }
}
```

The `data` wrapper is the standard Compos read-response envelope. Object hits
return the canonical Compos object ID in `id`, even when the engine uses a
workspace-qualified hash as its document ID. `object_id` also contains the
canonical ID when requested or when all attributes are returned. Selecting
only `id` does not expose this internal helper attribute. Other response fields
retain the Meilisearch response shape.

## Forbidden HTTP Shapes

The server must not expose:

- generic SQL endpoints
- generic JSON patch endpoints
- direct table CRUD over canonical tables
- direct adapter callback mutation endpoint that changes canonical state

Adapter callbacks append inbound evidence first. A dedicated Kernel reducer may
then emit canonical facts or command intents; adapters never mutate World state
directly.
