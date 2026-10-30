# Compos implementation issue ledger — 2026-09-05

Baseline reviewed: `210a46a`, `feature/rhiza-v0.9.1-port`.
Scope: code, current normative docs, and deployment render contracts.
This local ledger records every finding; it does not create GitHub issues.
No live deployment, data migration/reset, commit or push is part of this pass.

`Implemented` means locally changed; only `Verified locally` includes a focused
regression plus integration checks. Neither means deployed or HA approved.

| ID | Priority | Finding / completion criterion | State |
| --- | --- | --- | --- |
| C-01 | P1 | Multi-effect projection/activity pairing: one projection → its own event type; pagination and ReactiveRule candidates have no fan-out | Verified locally (SQLite/Rhiza, repeated pairing regression) |
| C-02 | P1 | Composite Function failure uses Task on_function_failed route/cancel, including output validation failure | Verified locally (SQLite/Rhiza route, invalid-output cancel and duplicate) |
| C-03 | P1 | Cancelled/satisfied Work does not accept late Workflow completion; legitimate workflow-child convergence remains intact | Verified locally (SQLite/Rhiza cancellation regression and existing workflow suite) |
| C-04 | P1 | ReactiveRule publish/disable/retire/delete enforce creator identity on SQLite and Rhiza; creator circuit breaker still works | Verified locally |
| C-05 | P1 | Operator cold-start launches quorum peers in Parallel; no per-pod PVC under the current product contract | Verified locally (render/CRD tests); live multi-node gate remains open |
| C-06 | P2 | work.open validates every distinct binding context_key root: active, workspace-scoped and correct relation-path type | Verified locally (SQLite/Rhiza alternate customer_id, missing/archived/foreign/wrong-type records) |
| C-07 | P2 | Known Rhiza availability/commit-unknown/durability failures have a safe retryable quorum_unavailable contract; preserve duplicate receipts | Verified locally (classification and existing idempotency suite) |
| C-08 | P2 | CLI remote logout failure preserves session for retry and returns failure | Verified locally |
| D-01 | P2 | Current docs distinguish Rhiza canonical SQL, SQLite local/test and derived Graph/Meilisearch | Updated and reviewed |
| D-02 | P2 | Remove contradictory PVC-required/operator-unsupported/current-main claims; distinguish render support, live topology and HA gates | Updated and reviewed |
| D-03 | P2 | Pin current runtime description to Rhiza 0.12; retain historical verification evidence as history | Updated and reviewed |
| D-04 | removal | Remove S2 remote audit sync from CLI, helpers, SDK and current docs; preserve local audit export/retry and historical journal decoding | Implemented and verified locally on 2026-09-06 after explicit user approval |
| D-05 | breaking cleanup | Remove export; rename CLI request tracking to ledger, including configuration, local format and Kernel causation attempt identity; no legacy aliases or migration | Implemented and verified locally on 2026-09-06; supersedes D-04 compatibility preservation |
| D-06 | ledger usability | Metadata-only read-only `ledger list` with state/workspace/limit filtering; discover attempt IDs without payload export | Implemented and verified locally on 2026-09-06 |
| C-09 | P1 | Recover unsent intents; distinguish unknown delivery from confirmed commitment; reject malformed/mismatched success receipts | Implemented and verified locally on 2026-09-06 |
| C-10 | P1 | Pin CLI retries to original server, principal and workspace; reject missing/mismatched identity before HTTP | Implemented and verified locally on 2026-09-06 |
| C-11 | P1 | Validate persisted intent hashes and idempotency key before replay; reject event-key collisions without overwriting append-only evidence | Implemented and verified locally on 2026-09-06 |
| C-12 | P1 merge gate | Concurrent Rhiza bootstrap sees schema_metadata without identity and fails startup | Fixed: metadata creation and identity insertion are the final atomic migration after all DDL; local three-node recovery passed |
| C-13 | P1 review | Conductor outbox selects 100 events but Rhiza status updates allow at most 64 SQL statements | Fixed: select at most 64; real Rhiza 65-event drain regression passed; PR #22 thread PRRT_kwDORkInVs6fk_K6 |
| C-14 | P2 review | Ordinary Rhiza command plans bypass the 64-statement preflight when no secondary Work effects exist | Fixed: apply the limit to every complete command plan before submission; PR #22 thread PRRT_kwDORkInVs6flE14 |
| C-15 | P2 review | CLI accepts receipts without workspace evidence and does not protect replay preconditions | Fixed: require workspace evidence and verify a complete command hash; full Go tests and vet passed; PR #22 threads PRRT_kwDORkInVs6flMyT / PRRT_kwDORkInVs6flMyU |
| C-16 | P2 review | Racing adapter inbound inserts reuse a Rhiza request ID with different generated arguments | Fixed: fresh transport request IDs with canonical row readback; real Rhiza concurrent regression, full Go tests and vet passed; PR #22 thread PRRT_kwDORkInVs6flMyV |
| C-17 | P2 review | SearchSyncer discovery rotation leaves previous workspace documents stale | Fixed: scoped cleanup completes before state switch; failure/retry regression, full Go tests and vet passed; PR #22 thread PRRT_kwDORkInVs6flTNz |
| C-18 | P1 review | Conductor inbound request dedup can bypass canonical payload verification | Fixed: shared canonical inbound verification; fresh inbound/publish transport IDs; real Rhiza regression, full Go tests and vet passed; PR #22 thread PRRT_kwDORkInVs6flZvW |
| C-19 | P2 local review | Equivalent Conductor result objects with reordered keys conflict on callback replay | Fixed: normalize result before hashing with UseNumber; reordered/changed/large-integer regressions, full Go tests and vet passed; independent re-review found no further actionable issue |
| V-01 | release gate | Authenticated live Backoffice multi-save/submit → search → ReactiveRule and new Conductor execution | Open; ordinary live user session/integration still required |
| V-02 | HA gate | Multi-node projector publication/crash-rebuild, Operator cold-start and empty-disk replacement on actual Kubernetes | Open; unit tests do not approve this gate |
| R-01 | interpretation | relation.link attaches an existing same-workspace typed record to scoped parent | Not a confirmed defect; do not require existing edge and make attachment impossible |
| R-02 | P2 follow-up | Same-Work concurrent Journal appends may return a generic constraint validation error instead of a classified conflict | Open; no atomicity loss established; narrow conflict classification requires evidence |

## Fix order and evidence

1. C-01 fixes only the event-to-activity pairing query, retaining the storage
   schema. The key is workspace + command + aggregate type + aggregate ID.
   Current multi-effect planner permits one mutation per existing aggregate
   and allocates distinct IDs for creates; parent Work has a different identity.
   If this producer invariant changes, add an explicit event linkage instead.
2. C-02/C-03 reuse the existing completion/failure policy and Work terminal
   guards; do not add mutable WorkflowRun tables or a second state machine.
3. C-04 enforces ownership before actor early returns and reasserts the Rhiza
   creator condition at commit; it does not grant new capabilities.
4. C-06/C-07/C-08 preserve record references, error convergence and retriable
   credentials, respectively.
5. C-05 changes generated desired resources only. Existing StatefulSets have
   immutable fields; this is not a safe in-place migration promise. Any future
   transition requires an explicit replacement plan and verified remote recovery.
   Existing volumes/data are not removed by this code change.
6. Docs express actual support boundaries. Historical deployment verification
   files remain unchanged; the new code is not silently declared deployed.

## S2 assessment

See [S2 and Rhiza boundaries](../adr/0004-s2-and-rhiza-boundaries.md). No new
queue or dependency is needed. The user subsequently approved removing the
optional S2 CLI feature; code/SDK removal is implemented, but no remote S2 data,
credential, service or local journal was deleted.

## Verification

Completed locally on 2026-09-06 KST:

- `go test ./... -count=1` — passed.
- `go vet ./...` — passed.
- `go test -race ./internal/kernel ./internal/server ./cmd/compos -count=1` — passed.
- `cargo test --locked --workspace --all-targets` — 8 passed, including generated
  CRD equality and Parallel/emptyDir/no-volumeClaimTemplates assertions.
- `cargo clippy --locked --workspace --all-targets -- -D warnings` — passed.
- `cargo fmt --all -- --check`, `helm lint deploy/charts/operator`,
  `git diff --check` — passed (Helm's optional icon recommendation only).
- Projection-pairing regression repeated ten times; real atomic Rhiza submission
  asserts no repeated aggregate identity in activity/projection effects.
- `go list -deps ./cmd/server ./cmd/search-syncer` contains neither the S2 SDK
  nor `internal/cli/commandlog`. Existing normal CLI submit without S2 and local
  audit retry tests remain green.
- Independent scoped final review found no additional blocker.

An initial new Rhiza pairing fixture failed setup validation; it was corrected
to seed the precise committed multi-effect shape before the successful final
suite. No production workaround or weakened check was introduced.

Live integration, actual multi-node Operator replacement, and graph HA gates
were not executed. The S3-backed three-node opt-in test is not included by an
unconfigured `go test ./...`; CI has a separate job for it. No live credentials
or secrets are recorded here.

After S2 removal, reverified locally on 2026-09-06 KST:

- `go mod tidy`, `go test ./... -count=1`, and `go vet ./...` — passed.
- `go test -race ./cmd/compos ./internal/cli/audit -count=1` — passed.
- CLI regression checks reject `audit sync` without journal or HTTP writes,
  preserve local export/retry, and retain historical bbolt journal decoding.
- `go list -deps ./cmd/compos ./cmd/server ./cmd/search-syncer` contains neither
  the S2 SDK nor `internal/cli/commandlog`.
- `sh -n scripts/package-cli.sh` and `git diff --check` — passed.
- No commit, push, deployment, or remote S2 resource cleanup was performed.

After the user's explicit ledger rename and compatibility-break request:

- Removed export and renamed recovery to `compos ledger retry --attempt ID`.
- Renamed the local package, path/env, storage namespace and Kernel causation
  identity; removed obsolete S2 reconciliation/cursor interfaces. Old audit
  files are rejected without altering their bytes. No migration or aliases.
- `go test ./... -count=1`, `go vet ./...`, and
  `go test -race ./cmd/compos ./internal/cli/ledger -count=1` — passed.
- Retained atomic append, fsync failure, corruption quarantine, and persisted
  retry coverage; old commands/options and export/sync rejection are tested.
- Narrowed the binary ignore rule to `/compos` so renamed CLI source files are
  not hidden by `.gitignore`; pre-existing untracked files were not removed.
- CLI and server require a coordinated upgrade. No commit, push or deployment
  was performed; existing local records were not deleted.

After ledger list and recovery corrections (2026-09-06 KST):

- `go test ./... -count=1`, `go vet ./...`, and
  `go test -race ./cmd/compos ./internal/cli/ledger -count=1` — passed.
- Real CLI smoke: `ledger list --status unknown --limit 5` with isolated missing
  ledger/session paths returned an empty list and created neither file.
- Focused coverage includes metadata-only filters/order, read-only byte stability,
  unsent recovery, pinned scope mismatch, invalid receipts, absorbing commitment,
  immutable event collision rejection, and unchanged large-integer JSON replay.
- Independent bounded re-review found no remaining blocker in the modified
  ledger paths. This is not an approval of the separate live/HA gates above.
- No commit, push or deployment was performed.

PR #22 follow-up (2026-09-06 KST):

- The failed three-node CI exposed a bootstrap race between metadata table
  creation and a separate identity insert. The final migration now publishes
  both atomically only after all other DDL; existing partial identities remain
  rejected. Native Rhiza migration-boundary regressions passed.
- `./scripts/e2e-rhiza-cluster.sh` passed with local MinIO: concurrent startup,
  competing writes, one stopped peer, fresh replacement and post-recovery write.
- Conductor batches now contain at most 64 updates. Real Rhiza/HTTP regression
  drains 65 events as 64 + 1, with no extra resend on a third cycle.
- Ordinary command batches now share the same 64-statement preflight; oversized
  plans are rejected before any command, journal, outbox or projection writes.
- Deployment-pin thread PRRT_kwDORkInVs6flE12 was declined for this source/merge
  scope: tg900 keeps its last verified release until a separately authorized
  coordinated build/deployment updates the digest and deployment evidence.
