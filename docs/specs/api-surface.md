# API Surface

Compos exposes command-first mutation APIs. CRUD routes over canonical business
tables are forbidden.

## Mutating API

The Kernel command execution endpoint is:

```text
POST /api/v1/kernel/commands
```

CLI writes must not call that endpoint before local durability. CLI writes
first append the command intent to the embedded local request ledger, then
submit the command to the Compos server. The server command endpoint does not
publish a second command log; no S2 audit synchronization command remains.

Every mutation requires:

- authenticated principal derived from credentials
- requested effective actor
- workspace id
- command type
- idempotency key
- target
- preconditions
- command-specific payload

The Kernel validates every mutation before committing.

## Forbidden Mutating Routes

These routes must not exist as canonical mutation APIs:

- `POST /issues`
- `PATCH /issues/{id}`
- `POST /projects`
- `PATCH /projects/{id}`
- `POST /activities`
- `POST /records`
- `PATCH /records/{id}`
- `PATCH /object_instances/{id}`

If compatibility is ever required, compatibility routes must be thin command
intent builders and must not write canonical rows directly.

## Read API

Canonical object and projection read responses declare consistency:

- `canonical_linearizable`
- `canonical_stale_bounded`

CLI reads use canonical server endpoints. Mutations are still revalidated by the
Kernel.

Human inboxes and work streams are read through
`GET /api/v1/workspaces/{workspace_id}/work` and
`GET /api/v1/workspaces/{workspace_id}/work/{stream_id}`. Status and assignee
are derived from immutable Journal facts rather than updated through CRUD routes.

Domain Event reads use `GET /api/v1/workspaces/{workspace_id}/events` and writes
use `event.emit`. Knowledge is externally owned; its former read and command
endpoints, including Concept traversal and grounding, have been removed.
Ontology revisions remain available through generic spec publication and reads.

## Import API

Imports are Kernel commands or read-only snapshots. Imports must not bypass
schema validation, authorization, state machines, relation constraints, command
ledger, activity, outbox, or projection events.

## Adapter Ingress API

Adapter callbacks may enter through append-only inbound event APIs. These routes
require bearer authentication and workspace membership, record external evidence
only, and must not change canonical business aggregate rows directly. Reducers
turn inbound events into observations or Kernel command
intents through the normal command path.
