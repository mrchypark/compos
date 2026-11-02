# tg900 authentication

Compos exchanges patch-data `users` tokens at `POST /api/v1/auth/pocketbase`
with JSON `{"token":"<patch-data token>"}`. Verified `example.invalid` emails are
allowed. Use the returned `data.session.token` as the Compos bearer token.

`COMPOS_BOOTSTRAP_WORKSPACE_NAME=Compos` provisions the Rhiza login workspace
atomically before serving requests and selects it for automatic enrollment.
Successful logins receive `member` membership, never automatic owner access.
Provisioning reuses the workspace creation and built-in schema paths under
the internal `system:bootstrap` principal. Repeated startup is idempotent.
This setting requires PocketBase mode and must not be combined with an explicit
`COMPOS_POCKETBASE_AUTO_WORKSPACE_ID`.

The deployment uses `emptyDir` for local state, with Rhiza GCS remote durability
set to `before-ack`. On 2026-10-08 the v0.12 application state was logically
migrated to Rhiza v0.19.0, latticedb-go v0.11.3, SQL execution policy 7, and
Compos schema v3. Bucket `compos-prod-602454948273-20260815`, object prefix
`compos`, cluster ID `v019-20261008-rehearsal3`, and node ID
`compos-v019-rehearsal3-0` are the live durable identity. The verified rehearsal
became production because the quiescent final export was byte-identical.
**Do not delete `compos/v019-20261008-rehearsal3/` or rename its identities.**

The old cluster `compos` with an empty object prefix remains an archive, not a
writer. Never open its physical artifacts with v0.19.0 or use an image downgrade
as rollback after new writes. The bounded offline tools preserved all 492 rows
in 30 application tables, including IDs, versions, sessions, counters, command
ledger, and Journal. The retired template table was empty. Full comparison also
passed after recovery from GCS into a fresh local directory. See the
[migration runbook and evidence](../../docs/plans/rhiza-v012-v019-logical-migration.md).

### v0.19.0 candidate verification — 2026-10-08

Commit `6e31513558bc6ca5e0ab8352dab8257eec0f118c` passed the full Go tests,
vet, CLI/server user journey, and hosted `test` and `rhiza-cluster` CI jobs.
The existing tg900 BuildKit published `compos-server`, `compos-cli`, and
`compos-search-syncer` images with tag `20261008-6e31513`.
The server digest is
`sha256:29fc5e3b453248a567b2093c47beb47d0def5d4591248bcc444a97bddfa49c9a`.

An isolated temporary Deployment used the existing GCS bucket with a fresh
test prefix, cluster ID, node ID, and `emptyDir`. It passed workspace creation,
dynamic object-type definition, object creation, state transition, reads, and
GCS `before-ack` cold recovery after all test Pods stopped. Recovery preserved
object IDs, values, and versions. Replaying the exact original command returned
the original receipt with `outcome=duplicate`; a new command also committed.
The temporary Deployment and its synthetic GCS objects were removed afterward.

That isolated test verified only a fresh single-node candidate. The subsequent
logical migration above replaced the production Compos and SearchSyncer images.
Commit `e223565` then fixed canonical object IDs in the search projection;
Conductor was unchanged. Mixed-version operation and multi-node GCS failover
remain unverified.

KSA `compos-conductor/compos-rhiza-gcs` uses GKE Workload Identity/ADC without
JSON keys. Deploy provisioned bucket-scoped `roles/storage.objectUser` for
`principal://iam.googleapis.com/projects/602454948273/locations/global/workloadIdentityPools/patch2-the-new-era.svc.id.goog/subject/ns/compos-conductor/sa/compos-rhiza-gcs`.
The manifest does not provision bucket IAM; verify that binding before rollout.

On 2026-09-05 the user approved resetting the old local workspace and clearing
the legacy bucket objects. The legacy `compos/compos` StatefulSet,
`compos/compos-s2-lite` and their `compos/compos-operator` are scaled to zero.
Do not re-enable them: their old `hiqlite/compos/7` and `compos/s2/7` writers
must remain retired. Renaming their backups does not convert them to Rhiza.
Legacy shutdown/IAM GitOps reconciliation in Deploy remains separate from this
Compos manifest; preserve the live shutdown when resolving that repository's
dirty-main gate. Meilisearch and patch-data are not retired.

Meilisearch uses `meilisearch.compos.svc.cluster.local:7700` with the isolated
`compos-tg900-objects` index. The `compos-search-auth` Secret contains a
`meilisearch-search-key` restricted to search on that index. Existing legacy
indexes and syncers are not changed.

## SearchSyncer

Apply `compos-conductor.yaml` before `search-syncer.yaml`. The separate
`compos-syncer-auth` Secret contains `compos-token` (random 32+ byte deployment
token) and `meilisearch-write-key`. The writer key is restricted to
`compos-tg900-objects` with `indexes.get`, `indexes.create`, `settings.update`,
`documents.add`, `documents.delete`, and `tasks.get`; it cannot search or access
legacy indexes. Provision keys using the Meilisearch keys API, never commit them.

`COMPOS_SEARCH_SYNCER_TOKEN` enables projection-only service authentication.
`COMPOS_SEARCH_SYNCER_WORKSPACE_ID` selects its one existing workspace, defaulting
to the login bootstrap workspace when omitted. Configure this only at startup.
The token permits exact GET requests to `/api/v1/search-syncer/workspace` and that
workspace's `/projection/snapshot` and `/projection/events` paths. It is not a
user session, workspace member, search credential, or command/admin credential.
Rotate it by replacing the Secret and rolling both Compos and Syncer.

With `COMPOS_WORKSPACE_ID` omitted, Syncer discovers the token's workspace every
cycle. When discovery changes workspace, it deletes only the previous workspace's
documents and waits for that task before resetting its snapshot/cursor. Failed
cleanup preserves the previous state for retry; unrelated workspaces are untouched.
It performs an initial snapshot, then polls projection events every five seconds.
A completed snapshot at sequence zero still transitions to events. The cursor
advances only after each Meilisearch task succeeds. Restart, index recreation,
or cursor expiration triggers snapshot rebuilding; no PVC is used.

Object search documents use an `obj_`-prefixed SHA-256 ID derived from workspace
and canonical object ID, because valid Compos IDs can contain `/`. The original
ID remains in `object_id`; the Compos search API returns it in `hits[].id`.
Snapshot, create/update, state patch, and deletion use the same mapping.
Knowledge is externally owned. Restart the search-syncer after upgrading: its
object-only snapshot rebuild clears legacy documents for the active workspace.
The API independently removes hits without a valid `object_id`; no index settings
update or syncer restart is required for search availability. It reports
`returnedHits` for the filtered page and omits engine totals and facets, which can
include legacy documents. Pages can be short or empty until rebuilding clears
those documents; offsets still refer to engine results.
Opening a v3 Compos database drops the three dedicated
Knowledge tables and their contents and advances schema identity to v4, preserving
shared specs and history. Stop old writers before upgrading; this is not a
mixed-version writer rollout.

Only the active workspace is cleared during snapshot rebuilding. Documents from
a previously reset workspace may remain in the index but are excluded by Compos's
mandatory workspace filter. Meilisearch itself must retain or reprovision its API
keys if its ephemeral data is reset; index recovery alone does not restore keys.

Opt-in real-engine checks use an isolated `compos-tg900-e2e-*` index:
`COMPOS_E2E_MEILISEARCH_URL` and `COMPOS_E2E_MEILISEARCH_KEY` must be set before
running `go test -count=1 -v ./cmd/search-syncer -run MeilisearchE2E`.
The test deletes only its own temporary index.
