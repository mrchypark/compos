# Canonical Storage Schema

The executable storage schema source of truth is `internal/store/schema.sql`.
`internal/store/store.go` applies it to an empty local SQLite database and rejects a
database whose exact schema identity does not match. There is no schema-version
migration path at server startup; schema changes are breaking and require
explicit recreation.

Do not hand-maintain column inventories here. For exact tables, columns,
indexes, triggers, and constraints, inspect `internal/store/schema.sql` or the live database
created by the current binary.

## Schema identity

Storage is fresh-only and breaking. An empty database receives the canonical
schema and `schema_identity`; an exact identity restart is a no-op. A legacy or
partially initialized database is rejected without mutation and must be
recreated.

## Canonical Ownership

The Go Kernel owns canonical business storage in SQL: embedded Rhiza SQL in
deployed mode and SQLite only in local/test mode. API clients, adapters and automations
must not write canonical rows directly.

## Breaking Object-Type Noun Contract

Every object type has three required naming values: `key`, `singular`, and
`plural`. `key` is the plural slug used for machine lookup and command-surface dispatch;
`singular` and `plural` are explicit names used by help and other user-facing
surfaces. There is no separate `slug` value and no fallback or inflection.
No pre-existing user-defined object types are migrated. New definitions must
supply the complete tuple explicitly. Use plural keys such as
`contracts`; the reserved built-in keys are `people` and `companies`.

The naming values do not replace canonical IDs. `object_type_id`, object
instance IDs, relation definition IDs, and relation IDs remain stable persisted
identifiers. A command surface may expose names for dispatch and prose, but
relations, preconditions, and stored references continue to use their stable
IDs where their contracts require IDs.

## Reader Model

This section is a reader map, not a schema table.

- Workspaces and creator principal audit fields remain control-plane metadata.
  They describe attribution and idempotency; they do not authorize business
  operations.
- `object_types` define business object schemas. `people` is the built-in
  dynamic object type for every human represented in workspace data; it uses
  ordinary `object_types` and `object_instances` and is not authentication
  authority. `companies` is also a reserved built-in object type for workspace
  companies.
- `object_instances` are the canonical storage model for business
  objects. Domain nouns are configuration, not built-in top-level tables or
  commands.
- Relation rows connect object instances under relation definitions; one-sided
  cardinality is enforced transactionally by canonical slot rows. Person
  instances use these ordinary relations when linked to business objects. The
  built-in `works_for` definition links `people` to `companies` through ordinary
  `object_relations`.
- The command ledger is the source of accepted command truth and idempotency
  outcomes.
- `spec_revisions` stores bounded-kind immutable Task, Function, Workflow,
  AgentDefinition (`participant`), Domain Event and Ontology revisions.
- `journal_records` stores append-only work, routing, function invocation,
  Workflow, Domain Event, submission and completion
  facts. TaskRun, Assignment, Handoff, FunctionRun and WorkflowRun are
  projections, not mutable canonical rows.
- Adapter inbound events and ReactiveRule evaluations are canonical evidence, not
  business aggregate commits.
- Knowledge is externally owned. Fresh schemas omit `knowledge_documents`,
  `knowledge_relations` and `ontology_groundings`. Opening a v3 database atomically
  drops these three dedicated tables (children first, with foreign keys enabled)
  and advances schema identity to v4. This intentionally deletes their contents.
  Shared spec, Journal, command and projection history is preserved, including
  historical Knowledge kinds and Ontology revisions. Other identities are rejected.
  Logical exports declare the retired tables as excluded; imports reject exports
  that still include them. Existing graph artifacts are left untouched.
  Full-text object search is a rebuildable Meilisearch view.

## Invariants

- Kernel command preconditions check aggregate versions before commit.
- Payload fields validate against the object type schema before object writes.
- `object.update` may change an object's `title` and/or replace its complete
  `values` payload. A replacement payload is not a merge and must retain all
  fields required by the object type schema.
- Object identity, object type, workspace, and creation metadata are immutable
  system fields outside `values`. An object's state is also outside
  `object.update`; state changes use `object.move`.
- Relation changes use registered Kernel commands; there is no generic relation
  mutation API.
- Ordinary person and company instances are created through `object.create` and
  link to business objects through existing relation definitions and
  `relation.link` / `relation.unlink` commands. The deterministic own workspace
  company object is created by bootstrap/provisioning.
- There is no `employment` object, table, or Kernel aggregate. Employment-like
  facts are represented, when needed, by ordinary `works_for` object relations
  from `people` to `companies`.
- V1 command handlers reject required relation definitions until an atomic
  create-and-link command contract exists.
- Person objects do not grant authentication, membership, roles, capabilities,
  or other authorization.
- Same command idempotency scope and same payload hash returns the original
  result; same scope and different payload hash returns `idempotency_conflict`.
- Automations may emit command intents only through the Kernel command path.

## Transaction Boundary

The Kernel storage path must commit the command ledger row, World mutation,
aggregate version increment, activity row, outbox row, projection event row and
related Journal facts as one transaction. If any part fails, none commit.

## Built-in Person Object And Principal Binding

- The `people` key is reserved with singular `person` and plural `people`, and
  uses ordinary `object_types` and `object_instances`. Every human represented
  in workspace data is a `person`.
- Person instances are created by the standard `object.create` command,
  including when login requests an internal person creation. They may be
  created independently of login and remain unbound.
- Person instances are normal objects after creation. `object.update` may edit
  their `title` (the person's name) and replace their `values`, including
  editable person payload fields. Full replacement values must still satisfy
  the `people` schema and retain any schema-required fields.
- A person's object identity, object type, workspace, and creation metadata are
  immutable system fields and are not part of `values` or accepted by
  `object.update`.
- The `companies` key is reserved with singular `company` and plural
  `companies`, and uses ordinary `object_types` and `object_instances`. Each
  workspace has one deterministic own workspace company object with id
  `obj_company_<workspace_id>`. Additional company instances
  may be created as normal objects of type `companies`.
- The `works_for` relation definition is reserved, points from built-in
  `people` to built-in `companies`, and uses ordinary `object_relations`.
  `relation_definition.define` cannot redefine `works_for`; `relation.link`
  and `relation.unlink` manipulate `works_for` rows like any other relation.
  `works_for` has no principal, session, membership, role, or authorization
  meaning.
- Person-to-business links use ordinary `relation_definitions`,
  `object_relations`, and the registered `relation.link` / `relation.unlink`
  commands. They have no auth meaning, and no separate `employment` object
  exists.
- Workspace creation is a control-plane provisioning operation, not a Kernel
  command. It atomically creates the workspace, built-in types and relation
  definition, and deterministic own company. It creates no creator person or
  person-to-company relation.
- Person objects do not authorize sessions, workspace membership, roles,
  capabilities, or business operations.
