# ADR 0001: Rust Kernel Runtime on Hiqlite with CLI and S2 Command Log

Status: superseded by [ADR 0002](0002-go-sqlite-runtime.md)

Date: 2026-06-08

The Rust server, Rust Kernel and Hiqlite canonical runtime described below were
removed by the breaking Go port. This document is retained only as historical
decision context and is not an implementation contract.

## Context

Compos is not a flexible database UI. Its product claim depends on treating
business changes as commands executed by a Kernel. Previous PocketBase-era
surfaces mixed product-domain tables, templates, generic records, and direct
CRUD-style mutation paths. That conflicts with the desired model:

```text
User action -> Command intent -> Kernel validation -> Canonical commit
Automation -> Command intent -> Kernel validation -> Canonical commit
```

The new stack decision is:

- keep the CLI in Go;
- move the server and canonical database path to Rust;
- use `mrchypark/hiqlite` current `main` as the strongly-consistent embedded
  Raft SQLite store;
- keep CLI reads on canonical Rust server endpoints instead of embedding a
  local business-state replica;
- use an embedded bbolt journal as the CLI-local command-intent audit store;
- use S2 streamstore as the cloud durable command-intent and receipt log;
- let a cloud control plane manage quorum membership and recovery.

## Decision

Compos will use a Rust Kernel as the only canonical writer. The Kernel commits
validated commands into hiqlite-backed canonical storage. Go CLI commands append
an immutable local command-intent record to an embedded bbolt audit journal
first, then submit the command to the Compos server. The server publishes S2
command-intent and receipt records around Kernel execution when S2 command
logging is configured and the requested actor is replay-supported. Server-side
repair and DR jobs can read S2 command intents/receipts for repair and recovery.
The journal path is the only CLI write-intent path, but it is not canonical
business state and is not a source of accepted command truth.

CLI reads use canonical Rust server endpoints. Every mutation still goes to the
Rust Kernel and is revalidated there.

Hiqlite learner-only is not the current CLI storage mode. It is attractive
architecturally, but a Go CLI would need Rust FFI or a managed Rust sidecar to
host it. That is more moving parts than the current read-cache requirement
needs.

Cloud quorum management owns hiqlite cluster membership, bootstrap material,
placement policy, backup/restore policy, and node recovery. Product commands do
not manage Raft membership directly.

## Source Of Truth

Canonical business truth:

- Rust Kernel command handlers;
- hiqlite committed state machine;
- Kernel command ledger, activity log, and transactional outbox.

Local evidence and operator trace:

- CLI-local bbolt command-intent journal;
- S2 command-intent and receipt streams;
- operator repair cursors and replay plans.

Not authoritative:

- S2 command-log records;
- cloud quorum metadata;
- adapter callbacks;
- automation evaluation results before they become Kernel command intents.

## Boundaries

The Go CLI may:

- build command intents;
- append local command-intent events;
- read canonical server endpoints;

The Go CLI must not:

- mutate canonical object, task, submission, relation, workflow, or completion
  state locally;
- submit a write intent to the server before the intent is durably appended to
  the embedded bbolt journal;
- replay audit rows directly into business tables;
- decide command authorization or state transitions;
- act as a hiqlite Raft member for canonical writes.

The Rust Kernel may:

- validate permissions, guards, state transitions, relations, and idempotency;
- execute commands received from the server command-log repair/replay path;
- commit canonical writes to hiqlite;
- emit activity, outbox, and projection events;
- reconcile CLI audit records with canonical command invocations.

The cloud quorum plane may:

- create, join, remove, drain, and recover hiqlite nodes;
- issue bootstrap tokens and node identities;
- maintain desired quorum topology;
- coordinate backup and restore.

The cloud quorum plane must not:

- execute business commands;
- bypass Kernel command handlers;
- rewrite business state.

## Failure Semantics

If CLI journal append succeeds but Kernel submission fails, the journal records
a failed or pending command attempt. No business state changes.

If Kernel commit succeeds but CLI does not receive the response, the command is
resolved by idempotency key and canonical command ledger lookup.

If the command-log provider is configured for a replay-supported actor and is
unavailable, the server rejects before Kernel execution. The local CLI journal
remains evidence of the failed attempt, but the command is not accepted and is
not an automatic replay queue.

Command submission is allowed only with the expected aggregate version known to
the user. The Kernel rejects stale versions.

If the embedded local audit journal is unavailable, CLI mutation commands fail
closed unless a future reviewed emergency mode records an equivalent durable
local command attempt bundle before submission.

If quorum is unavailable, the Kernel rejects writes and may continue serving
bounded reads depending on configured consistency policy.

## Consequences

This is a breaking architecture change. The legacy PocketBase server, `project`,
`issue`, `work-item`, `template`, and generic record mutation surfaces must not
be treated as the new product API.

Hiqlite should not be used as a transparent sharded SQL layer. If sharding is
introduced later, requests must carry explicit shard keys and ring epochs, and
cross-shard writes remain out of scope until a separate coordinator exists.

Hiqlite must be pinned to an immutable commit. The initial requested fork/main
revision observed during this reset is `26c6d22a`; any change to this revision
is a storage upgrade and requires explicit review, compatibility tests, and a
rollback note.

Hiqlite is pinned directly to `mrchypark/hiqlite` rev `26c6d22a`. Compos does
not vendor or patch Hiqlite for DR metadata; confirmed replay derives its base
watermarks by opening the restored backup SQLite object in the server recovery
path.

S2 is valuable as a streamable command log with managed durability, or through
s2-lite backed by object storage for drills and constrained deployments. The
embedded bbolt journal owns local CLI durability. Neither is a substitute for
the Kernel command ledger, and both must remain behind narrow command-intent
journal interfaces.

## Review Notes

This decision is based on the current public descriptions of:

- `mrchypark/hiqlite`, a fork of `sebadob/hiqlite`, described as an embeddable
  Raft-based SQLite/cache system with strongly consistent replicated execute
  queries and local reads.
- `s2-streamstore/s2`, described as a durable streamstore with managed S2 and
  s2-lite backed by object storage or local filesystem.

Those characteristics match the intended roles: hiqlite for canonical quorum
state, bbolt for local append-only CLI audit, and S2 for server-published
external ingest and recovery evidence.
