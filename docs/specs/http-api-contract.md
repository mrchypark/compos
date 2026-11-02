# HTTP API Contract

The Go server exposes a small command-first HTTP API. Business mutations go
through the Kernel command endpoint only.

The current Work read projection is:

```text
GET /api/v1/workspaces/{workspace_id}/work
GET /api/v1/workspaces/{workspace_id}/work/{stream_id}
```

The collection route is the authenticated principal's Human inbox. It returns
open `work.routed` facts targeting that human, excluding terminal work and a
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
authenticated callback appends `agent.succeeded` and `work.submitted`; a valid
`submission_valid` result also satisfies ordinary Work. Human and Agent workflow
routes remain open until the submission carrying the same `workflow_step_id` and
`workflow_route_id`;
parallel route submissions converge independently and the enclosing Workflow
performs final Work completion. Function nodes remain open until
`function.succeeded` or `function.failed`. Publishing a Function or Workflow
rejects any reachable composite Function↔Workflow execution cycle.

## Backoffice business processing

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
summaries. These authenticated workspace reads are canonical local reads, not
Meilisearch or Graph projections.

Task `data_bindings` is an optional list of at most 32 semantic record bindings.
UI layout, widgets and form drafts remain Backoffice-owned. Each binding has:

```json
{
  "id": "quote",
  "object_type_id": "<quote-type-id>",
  "schema_version": 2,
  "context_key": "object_id",
  "writable_fields": ["amount"],
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
handoff continues under the authenticated submitter recorded in the Journal,
with membership, Function capability and pinned object checks still enforced.

`work.submit` without `changes` still records a submission only. Optional
`changes` is a nonempty list of at most 32 bounded operations:

```json
{
  "submission": {"decision": "approved"},
  "workflow_step_id": "review",
  "workflow_route_id": "<route-id-from-inbox>",
  "changes": [{
    "binding_id": "quote",
    "command_type": "object.update",
    "target": {"aggregate_type": "object_instance", "aggregate_id": "<quote-id>"},
    "preconditions": [{"aggregate_type": "object_instance", "aggregate_id": "<quote-id>", "expected_version": 1}],
    "payload": {"values": {"amount": 120}}
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
identifier). A reference is the singleton JSON object `{"client_ref":"quote"}`;
plain strings remain existing server IDs. References are accepted in create
`relations`, `relation.link` endpoint fields and the `submission` value. They are
resolved only within this request; unknown, duplicate, malformed and cyclic
creation references fail before writing. Input order does not determine creation
order. All IDs are assigned using the parent command identity and original change
index before validation; the server orders dependent creates and preserves the
original index in errors. New link endpoints have server-known version 1; existing
endpoint/definition preconditions still follow the ordinary command contract.

For example, an existing customer is pinned in Work `context.customer_id`. The
`quote` binding traverses incoming `quote.customer`; the `item` binding traverses
incoming `quote.customer` then incoming `item.quote`. Both permit `object.create`
and `allow_title`. Schemas supply generated number fields, which are omitted from
the request. A payload fragment is:

```json
{
  "submission": {"quote_id": {"client_ref": "q"}},
  "changes": [
    {"binding_id":"item","client_ref":"i1","command_type":"object.create","target":{"aggregate_type":"object_instance"},"payload":{"object_type_key":"items","title":"Item 1","values":{},"relations":{"quote":{"client_ref":"q"}}}},
    {"binding_id":"quote","client_ref":"q","command_type":"object.create","target":{"aggregate_type":"object_instance"},"payload":{"object_type_key":"quotes","title":"Quote","values":{},"relations":{"customer":"<existing-customer-id>"}}},
    {"binding_id":"item","client_ref":"i2","command_type":"object.create","target":{"aggregate_type":"object_instance"},"payload":{"object_type_key":"items","title":"Item 2","values":{},"relations":{"quote":{"client_ref":"q"}}}},
    {"binding_id":"item","client_ref":"i3","command_type":"object.create","target":{"aggregate_type":"object_instance"},"payload":{"object_type_key":"items","title":"Item 3","values":{},"relations":{"quote":{"client_ref":"q"}}}}
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
{"client_refs":{"q":{"id":"<assigned-id>","version":1,"generated_values":{"number":"Q-001"}},"i1":{"id":"<assigned-id>","version":1,"generated_values":{"number":"I-001"}}}}
```

The Journal contains the resolved submission, so the next Workflow step receives
actual IDs. Backoffice replaces draft IDs/numbers only after a successful receipt.
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

The optional `COMPOS_AUTH_MODE=oauth2` mode accepts an IdP access token directly
and revalidates it through discovered RFC 7662 introspection plus OIDC UserInfo.
It requires `openid` and a separate configured application scope (e.g.
`compos.api`), preserves workspace authorization, and issues no Compos session.
See [OAuth2 resource-server contract](oauth2-resource-server.md) for configuration,
identity registration, client registration prerequisites, errors and limitations.

Clients authenticate with:

```text
Authorization: Bearer <credential>
```

In the default `rauthy` mode, the credential is an opaque Compos session token
issued after the Rauthy access-token exchange below. The API server hashes the
bearer credential, resolves it through `auth_sessions`, and derives
`authenticated_principal` from the stored session. Clients never send
`authenticated_principal`. The Go CLI reads this credential from its login
session; it does not expose per-command `--token` flags.

`COMPOS_AUTH_MODE=none` is an explicit private/local development mode. It has
no auth routes and treats requests as the deterministic `dev` principal. Each
command is scoped to its submitted workspace without relying on a persisted
`dev` membership. It must not be used for a public deployment.

Workspaces, principals, provider identities, sessions, and memberships remain
control-plane authority. Every human represented in workspace data is a
person created through `object.create` with the built-in dynamic `people`
object type, whose singular name is `person`. A dedicated control-plane
principal-person binding connects a principal to one person in a workspace; it
is not a generic object relation because a principal is not an
`object_instance`. Person objects, principal-person bindings, and ordinary
person-to-business relations do not authorize roles, memberships,
capabilities, business operations, or sessions. Each workspace also has a
reserved `companies` object type, deterministic own workspace company object,
and reserved
`works_for` relation definition from `people` to `companies`. There is no
`employment` object, table, or Kernel aggregate.

## Rauthy Login

The default CLI login path exchanges a current Rauthy access token:

```text
POST /api/v1/auth/rauthy
Content-Type: application/json
```

Request:

```json
{
  "access_token": "upstream-access-token",
  "workspace_id": "wks_..."
}
```

`workspace_id` is optional. If omitted, the configured automatic-enrollment
workspace is used; otherwise exactly one existing membership must be available.
The server POSTs the token to Rauthy's introspection endpoint using the
configured client credentials and requires `active: true`, the configured
audience (string or array), required scope, a current `nbf`, and a future
`exp`. It then reads userinfo and requires its `sub` to equal introspection's
`sub`. The principal is deterministically derived from issuer and subject.
Userinfo must supply a nonempty ASCII email with `email_verified: true`, and
its complete domain must exactly match `COMPOS_AUTH_ALLOWED_EMAIL_DOMAINS`.

Successful response:

```json
{
  "data": {
    "principal_id": "rpr_...",
    "person_id": "obj_...",
    "email": "user@example.com",
    "name": "Example User",
    "workspace_id": "wks_...",
    "workspace_memberships": ["wks_..."],
    "session": { "token": "csn_...", "expires_at": "4102444800" }
  }
}
```

The session expiry is the earlier of the Rauthy token `exp` and the Compos
session TTL. Invalid/missing tokens, inactive claims, wrong audience/scope,
expired/not-yet-valid claims, or mismatched userinfo subject return the normal
`401 unauthenticated` envelope. Unavailable or malformed upstream responses
return `502` for an invalid success or `503` for timeout/5xx. A requested workspace without membership is `403`; automatic
enrollment is permitted only for the configured fixed workspace and fails if it
does not exist.

## PocketBase Login

```text
POST /api/v1/auth/pocketbase
Content-Type: application/json
```

The closed request is `{"token":"opaque-token","workspace_id":"wks_..."}`;
`workspace_id` is optional. Compos posts to
`{COMPOS_POCKETBASE_URL}/api/collections/{COMPOS_POCKETBASE_AUTH_COLLECTION}/auth-refresh`
with the raw token in `Authorization` (without a `Bearer` prefix), follows no
redirects, and requires a successful response containing a refreshed token and
a record with nonempty `id` and ASCII `email` plus `verified: true`. Only the
refreshed token's future numeric `exp` is read, solely to cap the opaque Compos
session; its identity claims and all client-token claims are ignored. Identity
and principal keys are deterministic and collision-separated by canonical
PocketBase origin, collection, and record ID.

`COMPOS_POCKETBASE_URL` accepts canonical `http://` and `https://` base URLs.
With `http://`, the raw bearer token has no transport encryption; the operator's
deployment and network boundary are responsible for confidentiality. There is
no separate allow-insecure flag for this explicit upstream URL choice.

## Person Binding And Session Issuance

After provider verification and workspace selection, human login resolves the
workspace's principal-person binding before inserting the session:

- The auth control plane requires exactly one binding for the authenticated
  principal in the selected workspace. If it already exists, either provider
  exchange reuses it.
- If no binding exists, the authenticated provider request creates a new built-in
  `person` instance through the standard `object.create` command path, then
  creates the dedicated control-plane binding. The request has no `person_id`
  selection field. A person may remain unbound if binding or later session
  insertion does not complete.
- The verified email claim is accepted only when Rauthy userinfo has
  `email_verified: true` or the PocketBase record has `verified: true`;
  phone claims are unsupported. Any future internal controlled flow that
  binds an existing person must require an exact match against an immutable,
  trusted verified claim; similarity, display-name matching, mutable payload
  edits, and inferred identity are insufficient.
- Person objects link to business objects through ordinary relation definitions
  and `relation.link` / `relation.unlink`. Those relations are separate from the
  principal-person binding. The built-in `works_for` relation links a `person`
  object to a `company` object as an ordinary object relation; it is not an
  employment aggregate and has no auth meaning.
- Person objects, generic relations, and the principal-person binding grant no
  authentication or authorization. Workspace membership and other principal
  control-plane checks still govern session and command access.
- No auth session is returned unless the selected workspace has exactly one
  valid principal-person binding for the authenticated principal. Previously
  issued sessions remain unchanged until expiry or revocation.
- Once created, the principal-person binding is immutable in V1. It is outside
  the person object's payload and cannot be changed with `object.update` or
  generic relation commands.
- Login does not create `works_for` rows. Later principals who become workspace
  members and log in receive only the required person/binding path for session
  issuance unless another explicit relation command links them to a company.

```text
GET /api/v1/auth/session
Authorization: Bearer <session_token>

POST /api/v1/auth/logout
Authorization: Bearer <session_token>
```

`GET /api/v1/auth/session` returns the principal and its current workspace
memberships. `POST /api/v1/auth/logout` revokes the presented session and
returns `{"data":{"status":"logged_out"}}`; the same token is then rejected in
legacy `rauthy`/`pocketbase` modes. In `oauth2` mode, session inspection uses the
IdP bearer and logout returns `405` because token/session lifecycle belongs to
the RP and IdP, not a local Compos session.

| Mode | Auth routes |
| --- | --- |
| `rauthy` (default) | `POST /api/v1/auth/rauthy`, `GET /api/v1/auth/session`, `POST /api/v1/auth/logout` |
| `pocketbase` | `POST /api/v1/auth/pocketbase`, `GET /api/v1/auth/session`, `POST /api/v1/auth/logout` |
| `oauth2` | `POST /api/v1/auth/identity` (identity only; no membership/session), `GET /api/v1/auth/session`; logout returns `405` |
| `none` | No `/api/v1/auth/*` routes (they return `404`); requests use `dev` locally |

Administrative control-plane endpoints additionally require the authenticated
principal to be configured in `COMPOS_ADMIN_PRINCIPALS`, or a bearer token that
matches `COMPOS_ADMIN_BEARER_TOKEN` for cluster operator Jobs.

## Workspace Create

```text
POST /api/v1/workspaces
Authorization: Bearer <session_token>
Content-Type: application/json
```

This is a control-plane provisioning endpoint, not a Kernel command and not an
entry in `GET /api/v1/kernel/command-registry`. The Go CLI exposes it as:

```sh
compos workspace create [--server URL] --name NAME [--key KEY] [--idempotency-key KEY]
```

The server derives `creator_principal_id` from the bearer session. The HTTP API requires
`idempotency_key`; the CLI generates one when the flag is omitted. When `key` is
provided, the CLI and server trim it and ASCII-lowercase it before deterministic
idempotency hashing, request hashing, and storage.

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
- an owner `workspace_memberships` row for the creator principal;
- the reserved `people` and `companies` object types for the workspace;
- the deterministic own workspace company object;
- the creator `person` object;
- the dedicated principal-person binding for the creator;
- one ordinary `works_for` object relation from the creator person to the own
  workspace company;
- the idempotency/provisioning record.

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
    "creator_principal_id": "gpr_...",
    "owner_principal_id": "gpr_...",
    "creator_person_object_instance_id": "obj_creator_wks_...",
    "workspace_company_object_instance_id": "obj_company_wks_...",
    "works_for_relation_definition_id": "rdf_builtin_works_for_wks_...",
    "works_for_object_relation_id": "rel_creator_company_wks_...",
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

This endpoint is the Kernel execution boundary for trusted API callers. Go CLI write commands must not submit here before local
durability; they append to the embedded local request ledger, then submit to the
Compos server. Embedded Rhiza owns HA replication and recovery; the server does
not expose command-log repair or confirmed-replay endpoints.

The Go SQLite support mode accepts an 8 MiB JSON command envelope.

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
    "source": "cli",
    "attempt_id": "attempt_...",
    "event_id": null
  }
}
```

## Update Object

`object.update` updates a normal object instance, including a `person`. The
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
through `object.move`. The dedicated principal-person binding is control-plane
data outside the person object and cannot be changed by `object.update`.
Existing-person login selection uses immutable `person_binding_claims`; changing
a person's mutable `values.email` with `object.update` does not rewrite the
binding claim snapshot.

## Evolve Object Type Schema

`object_type.schema_evolve` publishes a complete immutable field-schema snapshot.
It targets one active, non-built-in `object_type` and requires that type's current
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
confirmed-replay HTTP surface. Ambiguous CLI submissions are retried from the
local request ledger with the original command ID, idempotency key, and attempt ID.
`ledger list` is local-only metadata inspection. Retry requires the original
server, authenticated principal and workspace. Invalid or mismatched receipts
remain outcome-unknown; only validated command receipts confirm commitment.
Remote S2 audit synchronization has been removed from the CLI. Local retry still
enters only through the Kernel command endpoint. Embedded Rhiza owns cluster
checkpoint bootstrap, replicated logs, and recovery.

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
- `401 unauthenticated`
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

Response:

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

The current CLI normal reads use the canonical object endpoints.

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
Authorization: Bearer <session_token>
```

Returns definition-backed CLI nouns for a workspace. The CLI uses this endpoint
before accepting `PLURAL_KEY create`, `PLURAL_KEY move`, `PLURAL_KEY get`,
`PLURAL_KEY link`, or `PLURAL_KEY help`; undefined keys are rejected before
audit or Kernel command submission.

Response:

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
          "id": "rdf_customer_contracts",
          "key": "customer_contracts",
          "version": 1,
          "from_object_type_id": "otp_customer",
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
is no separate `slug`, singular-key alias, fallback, or inflection. Built-in
keys follow the same rule: `people` and `companies` have singular names
`person` and `company`. No pre-existing user-defined object types are migrated
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
Backoffice owns screen layouts and widgets; Compos retains schemas, semantic
Task bindings and atomic submission validation. The CLI web console renders its
own schema-derived forms, not persisted HTML.

## Actor Capability Management

```text
POST /api/v1/workspaces/{workspace_id}/actor-capabilities
GET /api/v1/workspaces/{workspace_id}/actor-capabilities
```

These endpoints are administrative control-plane APIs. They require an
authenticated admin principal and manage scoped service actor capabilities
only. They are not business aggregate mutation APIs.

Grant request:

```json
{
  "principal_id": "svc_runtime",
  "requested_actor": {
    "type": "service",
    "id": "svc_import"
  },
  "command_type": "object.create",
  "causation_source": "api",
  "relation_definition_id": null,
  "idempotency_key_prefix": "svc_import/",
  "max_automation_recursion_depth": null
}
```

Grant response:

```json
{
  "data": {
    "id": "cap_...",
    "workspace_id": "wks_...",
    "principal_id": "svc_runtime",
    "requested_actor": {
      "type": "service",
      "id": "svc_import"
    },
    "command_type": "object.create",
    "causation_source": "api",
    "relation_definition_id": null,
    "idempotency_key_prefix": "svc_import/",
    "max_automation_recursion_depth": null,
    "created_at": "..."
  }
}
```

User actors cannot be granted scoped actor capabilities. Service capability
grants must not include `max_automation_recursion_depth`. Automation dispatch
does not use actor capability grants; it runs under the creator-owned
ReactiveRule authority. `relation.link` and `relation.unlink` capability
grants must include `relation_definition_id` so service actors cannot receive
workspace-wide relation mutation authority; non-relation grants must not
include `relation_definition_id`.

## Adapter Inbound Events

```text
POST /api/v1/workspaces/{workspace_id}/adapter/inbound-events
Authorization: Bearer <session_token>
```

The authenticated principal must be a member of `{workspace_id}`.

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

Revisionless connections remain the existing legacy service integration, not a
fallback for a new user-profile authorization path. These pins do not prove
profile ownership, consent or permission; user-profile resolution and GoAuthy's
optional credential extension are not implemented by this contract.

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
Authorization: Bearer <COMPOS_CONDUCTOR_CALLBACK_TOKEN>
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
Authorization: Bearer <COMPOS_CONDUCTOR_TOKEN>
X-Conductor-Workspace-Id: <workspace_id>
```

These endpoints are the narrow service boundary for Conductor's built-in
`provider=compos` connector. Both token and workspace scope are mandatory.
Mutation and arbitrary operation endpoints are intentionally absent; Compos
business writes remain Kernel commands.

## Search

```text
POST /api/v1/workspaces/{workspace_id}/search
Authorization: Bearer <session_token>
Content-Type: application/json
```

Full-text search proxied through Meilisearch. The authenticated principal must
be a member of `{workspace_id}`. Requires `COMPOS_MEILISEARCH_URL` server
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
