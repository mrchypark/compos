# ReactiveRule And Adapter Boundary

Automations and adapters are outside the canonical write boundary.

## ReactiveRule

Automations evaluate events and emit command intents only.

ReactiveRule commands and their durable evaluation/outbox records carry:

- `automation_id`
- triggering event id
- recursion depth
- command idempotency key
- rendered command hash
- evaluation record id

Automations must not write canonical rows directly.
ReactiveRule evaluation is recorded separately from command commits. An
evaluation may emit a command intent, but that intent still has to enter the
normal Kernel command path. Runtime code must dispatch emitted intents through
the Kernel executor instead of applying reducer or automation writes directly.

## ReactiveRule V1 Contract

User-authored ReactiveRules are Definition Layer records. Creating or
updating a rule is a creator-owned Kernel command. Dispatch runs under the
rule creator's principal and records the automation actor in command causation.

The minimum v1 rule definition includes:

- `workspace_id`
- `automation_id`
- `rule_key`
- `name`
- `status`: `draft`, `active`, `disabled`, or `retired`
- `created_by_principal_id`
- `updated_by_principal_id`
- `created_at`
- `updated_at`
- `definition_schema_version`
- `rule_revision_id`
- `definition_hash`
- `requested_actor`
- `trigger`
- `predicates`
- `action`
- `idempotency`
- `limits`

On the wire, `requested_actor.type` must be `automation`, and
`requested_actor.id` must match `automation_id`. Rule lifecycle commands must
be issued by the rule creator.

Rule revisions are immutable. Each revision has a new `rule_revision_id` and
`definition_hash`. Evaluation records always reference the exact revision used.

## Trigger Contract

Rules evaluate committed Kernel events only. They do not evaluate raw adapter
payloads as canonical facts.

The v1 trigger shape is:

```yaml
trigger:
  source: kernel_canonical_event
  event_type: object.created
  event_schema_version: 1
  aggregate_type: object_instance
  allowed_causation_sources: [api, automation, adapter_reducer]
  excluded_automation_ids: [self]
```

Rules may subscribe only to events emitted by the public Kernel `Registry()`,
excluding ReactiveRule lifecycle events, plus the scheduler-owned public
`work.deadline.reached` SLA fact. Hidden reducers such as Function/Agent result
recording and Workflow completion are not visible triggers. Rules must not
subscribe to arbitrary internal, admin, quorum,
credential, secret, or control-plane events. Event visibility is an
authorization boundary: an evaluator must not expose fields the rule author or
automation actor cannot observe.

## Predicate Contract

V1 predicates are JsonLogic objects evaluated by Compos's bounded Go evaluator.

```json
{
  "and": [
    { "===": [{ "var": "payload.object.state_key" }, "ready"] },
    { ">=": [{ "var": "payload.object.amount" }, 1000] }
  ]
}
```

Evaluator data:

- `payload`: canonical event payload.
- `target`: `{aggregate_type, aggregate_id, aggregate_version, projection_sequence}`.
- `metadata`: `{event_type, causation_source, causation_automation_id}`.
- `event`: `{id, workspace_id, event_type, payload, target, metadata}`.

Rules:

- the stored predicate must be a JSON object;
- operators must be from the pure JsonLogic allowlist implemented in code;
- `log`, unknown operators, malformed arity, and legacy `mode`/`clauses`
  predicates are rejected before storage;
- single-key objects inside predicates are treated as JsonLogic operators;
- legacy `mode`/`clauses` rows that already exist are skipped during dispatch
  instead of aborting the event drain;
- the rule matches only when evaluation returns the JSON boolean `true`;
- runtime JsonLogic evaluation errors are treated as non-matches;
- JsonLogic dotted `var` paths are used, not JSON Pointer;
- missing variables follow JsonLogic semantics and resolve as `null`;
- serialized predicate size and nesting depth are bounded before storage and
  evaluation.

## Action Contract

V1 automation actions may emit commands marked `automation_action_allowed` in
the command registry:
`object.create`, `object.move`, and `relation.link`:

```yaml
action:
  action_id: create_followup
  kind: emit_command
  command_type: object.create
  target_template: {}
  payload_template: {}
  preconditions_template: []
```

The action command type must be allowlisted for the automation actor. Templates
must not set `workspace_id`, `command_id`, `requested_actor`, `causation`, or
idempotency namespace fields. Those fields are system-rendered.

Template values are literal JSON unless they use the closed reference form:

```json
{"$from": {"source": "event.payload", "path": "/object/id"}}
```

`source` is limited to `event.payload`, `event.target`, or `event.metadata`.
`path` is a JSON Pointer. Missing template references fail validation instead
of rendering `null` implicitly.

V1 explicitly forbids automation actions that create, update, or delete
ReactiveRules, grant/revoke capabilities, modify workspace membership,
change adapter configuration, access secrets, or execute quorum/control-plane
operations.

## Idempotency Contract

Idempotency is system-owned and deterministic. V1 requires the user-authored
`idempotency` object to be empty; target-key customization is deferred until a
consumer needs it.

Effective namespace:

```text
workspace_id / automation_id / rule_revision_id / action_id
```

Effective key:

```text
namespace / triggering_event_id
```

The evaluation/outbox record stores `idempotency_namespace`,
`idempotency_key`, `emitted_command_id`, and `rendered_command_hash`. Replaying
the same event/rule/action must resolve through the Kernel command ledger.

## Evaluation And Dispatch Durability

Matched evaluations that emit a command intent are durable outbox records, not
only in-memory intents.

Minimum dispatch fields:

- `evaluation_id`
- `workspace_id`
- `automation_id`
- `rule_revision_id`
- `triggering_event_id`
- `action_id`
- `emitted_command_id`
- `rendered_command_hash`
- `idempotency_key`
- `dispatch_status`: `pending`, `dispatched`, `applied`, `rejected`,
  `retryable_failed`, or `dead_lettered`
- `dispatch_attempt_count`
- `last_dispatch_error_code`
- `last_dispatch_error_message`
- `created_at`
- `updated_at`

Unique key:

```text
(workspace_id, automation_id, rule_revision_id, triggering_event_id, action_id)
```

Crash recovery must reconcile pending or ambiguous dispatches through the Kernel
command ledger using command idempotency.

In the SQLite/Rhiza server runtime, a successful Kernel command response triggers a
best-effort automation dispatch drain starting at the committed projection
event. The dispatch drain evaluates active rules for committed projection
events, submits emitted command intents back through the normal Kernel command
path, and continues over follow-up projection events until no newer event
remains or the runtime drain limit is reached. A dispatch failure must not
rewrite the already-committed command receipt; pending or ambiguous evaluations
remain recoverable through command idempotency.

## Lifecycle And Limits

`enabled: true` is not sufficient lifecycle state.

Active rules evaluate only events committed after `enabled_at` and
`activation_event_cursor`, unless a separate explicit backfill workflow is
implemented.

Rules carry operational limits:

- `max_recursion_depth`
- `max_emits_per_minute`
- `max_emits_per_hour`
- `max_pending_dispatches`
- `failure_threshold`
- `circuit_breaker_status`
- `last_disabled_reason`

V1 rule definitions must include `max_recursion_depth`,
`max_emits_per_minute`, `max_emits_per_hour`, `max_pending_dispatches`, and
`failure_threshold`. Dispatch evaluates these limits before rendering a command
intent. If emitted command intents for the rule already reached
`max_emits_per_minute` or `max_emits_per_hour` within the runtime window, the
rule does not emit another intent for that event. If pending dispatches for the
rule are already at `max_pending_dispatches`, the rule does not emit another
intent for that event. If failed dispatches for the rule reach
`failure_threshold`, the runtime opens the rule circuit breaker and disables the
rule.

Effective recursion depth is:

```text
rule.max_recursion_depth
```

Limits are enforced by workspace, automation actor, and rule. Recursion depth
does not protect against broad event fan-out.

## Explicitly Deferred

V1 defers:

- multi-action rules
- branching action graphs
- scheduled or delayed triggers
- historical backfill
- cross-workspace automations
- external outbound side effects
- user-defined code or scripts
- regex/custom operators beyond the bundled JsonLogic implementation
- rule-local mutable state
- fan-out actions
- self-modifying automations
- automation-created rule grants
- secrets access
- user-facing retry UI beyond internal dead-letter/replay tooling

## Adapter Rule

Adapters append inbound events. Kernel-owned reducers decide whether inbound
events become:

- observations
- command intents

Inbound adapter event endpoints require bearer authentication and workspace
membership before appending.

Adapters must not write canonical rows directly.
The inbound adapter event ledger records adapter id, inbound event id, payload
hash, canonical payload JSON, local reducer status, and receive time. Appending
an inbound event never changes object or relation rows.

## Forbidden Actions

Automations and adapters must not:

- move objects directly
- mutate relation rows directly
- bypass Kernel authorization
- bypass Kernel validation
