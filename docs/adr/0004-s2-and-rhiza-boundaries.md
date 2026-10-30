# S2, Rhiza, and durable work delivery

Status: accepted removal, 2026-09-06. The user explicitly approved removing S2
and `compos audit sync`, including the CLI surface. Source baseline: `210a46a`
plus the local audit fixes and this removal; not yet a deployed release.

## Conclusion

Compos does not need an S2 queue for its current runtime. There is no such
dependency in the normal server write or delivery path today. Rhiza persists
canonical SQL, including the command ledger and SQL outboxes; existing workers
perform delivery and retries. Rhiza is not itself a replacement task scheduler.

| Responsibility | Current owner | Is S2 required? |
| --- | --- | --- |
| Canonical business commit, receipt, idempotency | Kernel + Rhiza SQL | No |
| Replication, checkpoint and empty-disk recovery | Rhiza + configured object store | No |
| Pending Function/Agent execution delivery | SQL outbox + Compos dispatcher | No |
| Technical execution retries | Conductor | No |
| Search feed and cursor recovery | projection_events + SearchSyncer | No |
| Before-submit CLI intent evidence | local bbolt audit journal | No |
| Off-device audit evidence reconciliation | Removed; no replacement service introduced | No |

## Evidence in the code

- Previously `runAuditSync` alone constructed the remote S2 journal. The command,
  dispatch/help entry, remote helpers, `internal/cli/commandlog` package and
  direct `s2-streamstore/s2-sdk-go` dependency have now been removed.
- Neither the CLI nor server/syncer build depends on that SDK anymore.
- `internal/kernel/rhiza.go` commits ledger/activity/outbox/projection effects
  atomically. `internal/server/conductor.go` dispatches committed outbox work.
- `cmd/search-syncer` takes a snapshot and consumes SQL projection events.
- The CLI keeps `ledger retry` on the normal Kernel path;
  retry uses the original command identity, not a direct storage replay.

## What Rhiza does not replace

An intent that never reached Compos is absent from server storage. Rhiza cannot
recover that intent after loss of the CLI machine. The local audit journal
therefore remains necessary under the existing CLI contract. Removing S2 also
removes its opt-in off-device audit copy. No CLI export or off-device backup
feature remains. The local request ledger is evidence, not business authority.

Do not remove `outbox_events`, `projection_events`, dispatch workers, cursor
handling, Kernel idempotency, or Conductor retries merely because Rhiza persists
their data. Storage durability and reliable delivery are different concerns.
Also do not confuse S2 stream storage with GCS/S3 object storage used by Rhiza.

## Removal and compatibility

Do not start or configure S2 for Compos. `compos audit sync` is now an unknown
command; callers must stop invoking it. In a subsequent explicit user request
on 2026-09-06, export was removed and the CLI group renamed to `ledger` because
it tracks request attempts rather than security auditing. Use `ledger retry
--attempt ID`; the old `audit` command and `--audit` flag are not aliases.
The user explicitly rejected compatibility preservation: local settings use
`COMPOS_LEDGER_PATH`, identifiers use `AttemptID`/`attempt_id`, and the new ledger
does not migrate or read legacy audit files. The Kernel causation field is also
`attempt_id`; CLI and server must be upgraded together. Old S2 environment
variables are no longer consumed by the CLI. The indirect `google/s2a-go`
package, if present for Google authentication, is unrelated to S2 streamstore.

Only repository code, tests, dependencies and current documentation were
removed/updated. Existing remote S2 data, credentials, services and local bbolt
files were not deleted. No runtime deployment or remote cleanup is authorized
by this code-removal pass.

The tg900 one-node deployment has remote durability and restart recovery, not
multi-node write availability. S2 removal does not strengthen that guarantee.
