# Command Registry

The executable command registry source of truth is
`internal/kernel/registry.go`:

- `Registry()` returns the command metadata.
- `Definition` defines the metadata shape.
- `AutomationActionAllowed` marks the automation action allowlist.

The runtime metadata endpoint is:

```text
GET /api/v1/kernel/command-registry
```

That endpoint is served from `internal/server/server.go` and serializes the
same registry data. Do not hand-maintain payload schemas,
preconditions, authorization policy text, emitted events, or automation action
flags in this doc.

## Envelope

All commands use `CommandEnvelopeV1`; see `kernel-command-contract.md`.
`command_type` is executable only when it is registered in Go and has a
Kernel handler, validation, authorization, precondition, and storage contract.

## Reader-Facing Command Index

This list is intentionally reader-facing. It comes from
`Registry()` in `internal/kernel/registry.go`; refresh it
from that function or the runtime endpoint when the executable registry changes.

- `spec.publish`
- `work.open`
- `work.route`
- `work.submit`
- `work.cancel`
- `function.invoke`
- `event.emit`
- `knowledge.propose`
- `knowledge.review`
- `knowledge.accept`
- `knowledge.remove`
- `object_type.define`
- `object_type.schema_evolve`
- `relation_definition.define`
- `object.create`
- `object.update`
- `object.move`
- `relation.link`
- `relation.unlink`
- `reactive_rule.define`
- `reactive_rule.publish_revision`
- `reactive_rule.disable`
- `reactive_rule.retire`
- `object_type.delete`
- `object.delete`
- `relation_definition.delete`
- `reactive_rule.delete`

## Automation Action Allowlist

This list is intentionally reader-facing. It comes from registry entries whose
`automation_action_allowed` flag is `true`.

- `object.create`
- `object.move`
- `relation.link`

ReactiveRule lifecycle commands are registered Kernel commands, but they are
not automation actions in v1. Automations may emit allowlisted command intents;
they must not write canonical rows directly.

`object.update`, `relation.unlink`, `function.invoke`, and `event.emit` are
registered, but are not automation actions in v1.

`object_type.schema_evolve` is not an automation action.

Workspace creation is intentionally absent from this registry.
`POST /api/v1/workspaces` and `compos workspace create` are control-plane
provisioning surfaces, not Kernel commands.

## Object-Type Noun Contract

`object_type.define` requires the complete noun tuple in its payload:

```json
{
  "key": "contracts",
  "singular": "contract",
  "plural": "contracts"
}
```

`key` is the plural slug and the dynamic CLI dispatch key. `singular` is used
for prose about one object, while `plural` names the collection command group.
All three values are explicit. The registry and CLI must not accept a separate
`slug`, infer either name, accept an alias, or fall back to a singular key. No
pre-existing user-defined object types are migrated. New definitions use plural
keys such as `contracts`; built-ins use `people` and
`companies`.

These names are not canonical aggregate IDs. Registry metadata and command
surfaces continue to expose stable `object_type_id` values, and persisted
objects, relations, and preconditions retain their stable IDs.

## Adding A Command

Before adding a new command, update the executable sources first:

1. Add the `Definition` entry in `Registry()`.
2. Add command-specific validation when registry schema validation is not
   sufficient.
3. Add payload validation, authorization, precondition, storage, ledger, and
   projection behavior in the Kernel execution path.
4. Add tests that prove the registry endpoint and command execution path agree.
