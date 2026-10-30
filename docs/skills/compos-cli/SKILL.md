---
name: compos-cli
description: Use when an agent must operate the Compos CLI safely: login, inspect session state, create/read/move objects, define object types, run small user-flow smoke tests, retry ambiguous requests, or avoid operator-only disaster-recovery commands.
---

# Compos CLI

Use `compos` as a command-first client. Business reads go to the canonical server; `ledger list` reads only the local request ledger. Writes must append the local bbolt request ledger first, then submit through the server command path.

## Guardrails

- Never pass Compos session bearer tokens on command lines. `--token` is removed; use the one-shot configured-provider token exchange only when login is required.
- Never use removed local replica flags such as `--replica` or `--canonical`.
- Treat Rhiza checkpoint recovery and cluster membership changes as operator-only actions.
- Do not use `compos capability ...` or `compos automation-rule ...`; those are not user CLI commands.
- For shared clusters, do not run destructive scripts or scale/delete pods unless the user explicitly asks.
- After every write, verify the response is applied/accepted. Network/read/408/429/5xx failures are ambiguous and should be retried with the stored request intent (`compos ledger retry --attempt ...`).

## Required State

Run this first when paths are unknown:

```sh
compos setup
compos version
```

Useful per-agent isolation. The request ledger is the CLI's local crash/network evidence before the server call; normal users can rely on the default path.

```sh
export COMPOS_SESSION_PATH=/tmp/compos-session.json
export COMPOS_LEDGER_PATH=/tmp/compos-ledger.db
```

Do not configure S2. Remote audit synchronization has been removed; use local
`compos ledger list` for request metadata and `compos ledger retry` for recovery.
No export or remote synchronization command is provided.

## Login

Local/default server:

```sh
compos login \
  --server http://127.0.0.1:7878 \
  --workspace wks_default \
  --provider rauthy \
  --access-token "$RAUTHY_ACCESS_TOKEN"
```

Use `--provider pocketbase --access-token "$POCKETBASE_TOKEN"` for a
PocketBase-configured server.

The server accepts an operator-configured `http://` PocketBase upstream without
another allow-insecure flag. That choice sends the raw bearer token without
transport encryption, so use it only where the deployment and network boundary
protect token confidentiality.

For local composition, start the server with `COMPOS_AUTH_MODE=none` and use a
credential-less session for the deterministic `dev` principal. This mode has no
login or session routes and must not be used to emulate multi-user behavior.

Inspect and clear session:

```sh
compos whoami
compos logout
```

Start the local read-only console:

```sh
compos web --listen 127.0.0.1:8787 --server http://127.0.0.1:7878 --workspace wks_default
```

## Workspace Creation

Create a workspace from the logged-in session:

```sh
compos workspace create \
  --server http://127.0.0.1:7878 \
  --name "Acme Research" \
  --key acme-research
```

`workspace create` posts `POST /api/v1/workspaces` with the session bearer
token. It is a control-plane provisioning call, not a Kernel command, and it
does not use the local command request ledger. The CLI generates an
idempotency key when `--idempotency-key` is omitted. When `--key` is supplied,
the CLI and server trim it and ASCII-lowercase it before deterministic hashing
and provisioning. The endpoint creates the workspace, owner membership, creator
person, principal-person binding, optional immutable email claim row when the
creator identity has a verified email, deterministic workspace company, and
creator `works_for` relation atomically/idempotently. The route is implemented
for both local SQLite and embedded Rhiza storage. The response `created_at` value
is the stored Unix-seconds string.

## Normal User Flow

Mutation commands accept `--server`, `--workspace`, `--actor`, and `--idempotency-key`. Omit `--actor` for normal user work so the CLI uses the logged-in principal.

Define a type:

```sh
compos object-type define \
  --server http://127.0.0.1:7878 \
  --workspace wks_default \
  --key contracts \
  --singular contract \
  --plural contracts \
  --field-schema '{"properties":{"slot":{"type":"number"},"owner":{"type":"string"}}}'
```

`--key`, `--singular`, and `--plural` are all required. `--key` is the plural
slug used for dynamic dispatch. The short `compos define` form accepts the same
flags; it has no positional-key form. No pre-existing user-defined object types
are migrated. Do not pass a separate slug, use an alias, or infer, singularize,
or pluralize any name.

Create an object:

```sh
compos object create \
  --server http://127.0.0.1:7878 \
  --workspace wks_default \
  --type contracts \
  --title "first task" \
  --values '{"slot":1,"owner":"user1"}'
```

Read one object:

```sh
compos object get \
  --server http://127.0.0.1:7878 \
  --workspace wks_default \
  --id obj_...
```

List objects:

```sh
compos objects list \
  --server http://127.0.0.1:7878 \
  --workspace wks_default \
  --type contracts
```

Move an object with an expected version:

```sh
compos object move \
  --server http://127.0.0.1:7878 \
  --workspace wks_default \
  --id obj_... \
  --version 1 \
  --to-state review \
  --reason "ready for review"
```

Use `--force` only for an audited manual override of the transition rule.

Update an object title or replace its values with an expected version:

```sh
compos object update \
  --server http://127.0.0.1:7878 \
  --workspace wks_default \
  --id obj_... \
  --version 1 \
  --title "renamed task" \
  --values '{"slot":2,"owner":"user1"}'
```

Pass `--title`, `--values`, or both. `--values` is a full replacement object,
not a patch; include every schema-required field. There is no dynamic
`PLURAL_KEY update` alias yet, so use `compos object update` for `people`,
`companies`, and other object types.

## Object-Type Word Alias

After `contracts` exists, these are equivalent to the generic object commands:

```sh
compos contracts create --workspace wks_default --title "second contract" --values '{}'
compos contracts get --workspace wks_default --id obj_...
compos contracts move --workspace wks_default --id obj_... --version 1 --to-state review
compos help contracts
```

## Built-in Person CLI

Every human represented in workspace data is a built-in dynamic `people`
object type with singular name `person`. Person instances are normal objects
and use the standard object commands for creation, discovery, and inspection:

```sh
compos object create --workspace wks_default --type people --title "Ada Lovelace" --values '{}'
compos help people
compos people help
compos objects list --workspace wks_default --type people
compos people get --workspace wks_default --id obj_...
```

The Kernel's `object.update` command and the Go CLI's `compos object update`
command can edit a person's object `title` (name) and replace its `values`
payload. Replacement values are complete, not a partial merge, and must retain
all fields required by the latest schema; a values replacement repins the
object to that schema, while a title-only update retains its current pin. Object identity, object type,
workspace, and creation metadata are immutable system fields outside the
payload. The principal-person binding is also outside the payload and
immutable. Do not recreate a person to edit these fields. There is no
high-level `people update` alias; use `compos object update`.

The principal-person binding is not a generic relation and grants no
authorization. Use the normal relation definition/link/unlink commands below
to connect a person to business objects through `relation.link` and
`relation.unlink`; `people` resolves through the dynamic object command
surface.

Every workspace also has a reserved `companies` object type with singular name
`company`, deterministic own
workspace company object, and reserved `works_for` relation definition from
`people` to `companies`. `works_for` rows are ordinary object relations. There is
no `employment` object, table, or Kernel aggregate, and later principals who
join or log in are not automatically linked to the workspace company.

## Relations

```sh
compos relation define \
  --workspace wks_default \
  --key parent_child \
  --from-type ot_... --from-type-version 1 \
  --to-type ot_... --to-type-version 1 \
  --cardinality one_to_many

compos relation link \
  --workspace wks_default \
  --definition rdf_... --definition-version 1 \
  --from obj_... --from-version 1 \
  --to obj_... --to-version 1

# Built-in works_for: person -> company, still an ordinary relation.link.
compos relation link \
  --workspace wks_default \
  --definition rdf_builtin_works_for_wks_default --definition-version 1 \
  --from obj_person --from-version 1 \
  --to obj_company_wks_default --to-version 1

compos relation unlink \
  --workspace wks_default \
  --definition rdf_... \
  --id rel_... \
  --version 1
```

## Request Ledger

List local mutation requests without login or network access; payloads and raw
error bodies are not displayed. Status is `unsent`, `unknown`, `rejected`, or
`committed`. Retry unsent/unknown requests using their `attempt_id` after selecting
the original server, principal and workspace. Missing or mismatched identity
pins are rejected. Old audit commands, configuration, and files are not migrated.

```sh
compos ledger list --limit 20
compos ledger list --status unknown --workspace wks_default
compos ledger retry --attempt attempt_...
compos ledger retry --journal /tmp/compos-ledger.db --attempt attempt_...
```

## Operator-Only Commands

Capability grants are admin/control-plane operations. Automation-rule lifecycle
changes are creator-owned Kernel commands exposed through the API, not the
`compos` user CLI.

Retry an ambiguous submission from the local bbolt request ledger using the
original command and idempotency key (this does not repair corrupt journals):

```sh
compos ledger retry --limit 20
```

Database recovery is not a CLI or HTTP command. The Rust Operator preserves
stable membership inputs; embedded Rhiza restores checkpoints and catches up
replicated logs from shared object storage. Local data may be disposable.
