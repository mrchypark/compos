# Kernel Command Contract

Every business mutation in Compos is a Kernel command.

## Command Envelope

```json
{
  "command_id": "018f9c5e-...",
  "idempotency_key": "...",
  "workspace_id": "...",
  "requested_actor": {
    "type": "user|service|automation",
    "id": "..."
  },
  "command_type": "registered wire name; see command-registry.md or the runtime registry endpoint",
  "target": {
    "aggregate_type": "registered aggregate type",
    "aggregate_id": "..."
  },
  "preconditions": [
    {
      "aggregate_type": "registered aggregate type",
      "aggregate_id": "...",
      "expected_version": 12
    }
  ],
  "payload": {},
  "causation": {
    "source": "api|automation|adapter_reducer",
    "attempt_id": "local_attempt_id_or_null",
    "event_id": "optional_prior_event",
    "automation": {
      "automation_id": "auto_...",
      "evaluation_record_id": "aev_...",
      "recursion_depth": 1,
      "idempotency_namespace": "wks_.../auto_.../arv_.../action_id",
      "effective_permission_policy": "owner_principal"
    }
  }
}
```

`causation.automation` is required only for internal automation dispatch with
`requested_actor.type = "automation"`. User and service commands omit it.

## Required Kernel Checks

- idempotency key uniqueness;
- actor authentication;
- authorization policy;
- workspace membership;
- aggregate version;
- object type and field definitions;
- relation cardinality and required relation constraints;
- state machine transition rules;
- validation rules;
- automation recursion limits;
- audit/activity/outbox transactionality.

Object state machines use `initial` plus `transitions`. `transitions` is a map
from each concrete state to its allowed target states. Terminal states are
explicit empty arrays, and `["*"]` means the source state may move to any state
key defined in the same `transitions` map. `object.move` may set `force: true`
to bypass the transition edge check, but the target state must still be defined;
the resulting `object.moved` event records
`forced`, `forced_by_principal_id`, and `forced_at`.

```json
{
  "state_machine": {
    "initial": "PENDING",
    "transitions": {
      "PENDING": ["APPROVED", "REJECTED"],
      "APPROVED": ["PROCESSING", "CANCELLED"],
      "PROCESSING": ["DONE", "FAILED"],
      "DONE": [],
      "FAILED": ["RETRYING"],
      "REJECTED": [],
      "CANCELLED": [],
      "RETRYING": ["PROCESSING"]
    }
  }
}
```

For `service` requested actors, the Kernel also checks an explicit scoped actor
capability. Automation actors instead require internal dispatch authority
derived from a creator-owned ReactiveRule:

- authenticated principal id;
- requested actor id and type;
- workspace id;
- command type;
- causation source;
- idempotency namespace prefix;
- automation recursion depth, when applicable.

## Command Registry

`command_type` is not arbitrary text. Every command type is registered with:

- stable wire name;
- payload schema;
- allowed target aggregate types;
- required preconditions;
- authorization policy;
- idempotency behavior;
- emitted activity, outbox, and projection events.

Unknown command types fail closed.

Presentation templates are not Kernel commands or canonical data. Clients
owns layouts and widgets; Compos owns field schemas, semantic data bindings and
submission validation. The retired `object_type.template_set` command is rejected
as unregistered, without a compatibility alias.

`object.update` targets an existing `object_instance`, requires the matching
object precondition, and accepts `title`, `values`, or both. `values` is a full
replacement object, not a merge. The handler rejects system fields such as
`id`, `workspace_id`, `object_type_id`, `object_type_key`, `state_key`,
`version`, and creation/update metadata inside `payload.values`; state changes
remain `object.move`.

A title-only update preserves the object's existing schema revision pin. Any
update containing `values` validates the complete replacement against the
latest immutable revision of its object type and repins the object to that
revision. Clients cannot select a schema revision: `schema_version` and
`schemaVersion` are unknown fields.

Workspace creation is not a Kernel command. `POST /api/v1/workspaces` is a
control-plane provisioning endpoint and therefore has no `workspace.create`
entry in this registry.

## Payload Schema

`payload` is JSON only at the transport boundary. Each command has a schema.
Unknown fields fail closed unless explicitly declared as extension fields for
that command.

`object_type.define` requires explicit noun naming in addition to its schemas:

```json
{
  "key": "contracts",
  "singular": "contract",
  "plural": "contracts",
  "field_schema": {},
  "state_machine_id": null
}
```

`key` is the plural slug. `singular` and `plural` are required independent
values; the Kernel contract does not infer them and does not accept a separate
`slug` or aliases. No pre-existing user-defined object types are migrated. New
definitions use plural keys such as `contracts`.

Names are lookup and presentation metadata, not canonical IDs.
`object_type_id` and all existing aggregate and relation IDs remain stable and
continue to be used wherever command targets, preconditions, or stored
references require IDs.

## Idempotency

Idempotency scope is:

```text
(workspace_id, principal_id, idempotency_key)
```

Same scoped key and same payload hash returns the original result. Same scoped
key and different payload hash returns `idempotency_conflict`.

## Command Outcomes

Result codes:

- `applied`
- `duplicate`
- `idempotency_conflict`
- `precondition_failed`
- `forbidden`
- `validation_failed`
- `schema_mismatch`
- `quorum_unavailable`
- `automation_recursion_limit`

## Transaction Boundary

Canonical aggregate write, aggregate version increment, command ledger row,
activity row, outbox row, and projection event are committed in one canonical SQL
transaction (Rhiza atomic statements in deployed mode, an interactive SQLite
transaction in local/test mode). If any part fails, none of them commit. A
bounded `work.submit.changes` may emit one effect per affected aggregate under
the same parent command; consumers must pair events by aggregate identity,
not by command ID alone.

## ReactiveRule

ReactiveRule output is a command intent:

```text
When event
If predicate
Then emit command intent
```

Automation never writes canonical rows directly.

## Adapter Rule

Adapter callbacks enter as inbound events. A Kernel-owned reducer may transform
them into observations or command intents. The adapter itself never mutates
canonical rows.
