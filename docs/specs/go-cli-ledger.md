# Go CLI Request Ledger

The Go CLI keeps an embedded bbolt append-only command-intent journal. This
local journal is the first durable record for CLI writes. The Compos server
does not publish a second command log. Remote S2 audit synchronization has been
removed. `compos ledger list` inspects local attempts; `compos ledger retry`
recovers eligible attempts through the Kernel.

## Responsibilities

- record every CLI command intent before any Kernel submission;
- write local journal events before any server/Kernel call;
- preserve command payload hashes and idempotency keys;
- reconcile attempts with canonical Kernel command receipts;
- expose metadata-only request status without requiring login or network access;
- retry ambiguous submissions through the Kernel command path.

## Non-Responsibilities

- no business state authority;
- no direct replay into canonical tables;
- no direct CLI write submit to the Kernel before local journal append;
- no permission or transition decisions;
- no quorum membership decisions.

## Append-Only Event Interface

```go
type CommandAttemptJournal interface {
    AppendAttempt(ctx context.Context, event AttemptAppended) (AttemptID, error)
    AppendSubmitted(ctx context.Context, event AttemptSubmitted) error
    AppendRejected(ctx context.Context, event AttemptRejected) error
    AppendCommitted(ctx context.Context, event AttemptCommitted) error
}
```

Mutating CLI commands must write to the embedded local journal before they call
the server command endpoint. Tests may use an in-memory fake at this boundary.

## Write Path

1. CLI constructs a command intent.
2. CLI appends `AttemptAppended` to the embedded local journal first.
3. CLI records `AttemptSubmitted` immediately before attempting HTTP delivery to
   `POST /api/v1/kernel/commands`. This is not proof of server receipt.
4. The CLI records a failure or a validated Kernel receipt. Only a valid receipt
   for the original command can confirm commitment. HTTP 2xx alone cannot.

`ledger retry` resubmits only through the Kernel command endpoint. Embedded Rhiza
checkpoints and replicated logs own SQL recovery. Rhiza persists SQL outboxes,
while existing workers own delivery/retry. It cannot recover a CLI intent that
never reached the server. See the [S2 removal decision](../adr/0004-s2-and-rhiza-boundaries.md).
The request ledger uses a new local format. Old audit files are not migrated
or deleted, and cannot be used as a ledger.

## Local Request Recovery

The CLI exposes the local journal through:

```text
compos ledger list [--journal PATH] [--status STATUS] [--workspace ID] [--limit N]
compos ledger retry [--journal PATH] [--attempt ID] [--limit N] [--server URL]
```

`list` returns metadata only: attempt ID, timestamps, server, workspace, command
type, status, failure code/reason, and retry eligibility. It does not expose
payloads, credentials, or raw server error bodies. Filters apply to the derived
attempt state, not individual events. The default limit is 20, newest first;
batch retry processes oldest eligible requests first.

`audit`, `export`, `show`, and `sync` are not supported CLI commands.
The ledger records CLI mutation attempts, not every HTTP request or a security audit.
Configure `COMPOS_LEDGER_PATH` or use the new default ledger path. The internal
package is `internal/cli/ledger`; identifiers use `AttemptID` and `attempt_id`,
including Kernel command causation. There are no old-name aliases or migration.

`ledger retry` is the user-facing recovery path for unsent and ambiguous local
submissions. It resubmits
the stored `CommandIntent` with the original attempt id, command id, and
idempotency key; it must not synthesize a new command.

Each intent pins the normalized server URL (including any path prefix) and
authenticated principal, alongside its workspace. Retry validates these against
the selected session before any HTTP call. `--server` cannot redirect a stored
request to a different server. Records without identity pins are not retried;
there is no compatibility fallback or automatic migration.
Before replay, the stored complete command hash (including preconditions),
idempotency key, and payload, target, and actor-context hashes must match the
decoded intent. Missing or mismatched integrity metadata
blocks delivery instead of constructing a replacement request.

## Derived Status

Attempt status is a local projection over immutable journal events. No
implementation may overwrite an attempt row to change status.
Duplicate event keys are rejected atomically rather than overwriting earlier
evidence. A confirmed commitment remains terminal if a late failure is recorded.

| Display status | Local evidence | Retry |
| --- | --- | --- |
| `unsent` | Intent recorded, no delivery attempt recorded | Yes, with matching scope |
| `unknown` | Delivery attempted, network/read/transient HTTP failure, or invalid receipt | Yes, with matching scope |
| `rejected` | Definitive rejection | No |
| `committed` | Validated Kernel receipt | No |

`committed` confirms the command, not completion of a Function, Work or Workflow.
The receipt must contain the original command ID, a successful outcome, and
nonempty workspace evidence matching the pinned workspace.
Invalid receipts remain unknown and are not automatically retried in a tight loop.

## Fail-Closed Mutation Rule

If the CLI cannot durably append `AttemptAppended`, the CLI must not submit or
forward the command.

## Corruption Handling

Read-only listing reports undecodable records without changing the file. On the
write/retry path, detecting an undecodable indexed event records a
`ledger_quarantine` marker. A quarantined journal must reject future append
attempts until rebuilt or explicitly recovered. Neither listing nor retry is a
corruption-repair command.
