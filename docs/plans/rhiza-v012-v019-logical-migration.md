# Rhiza v0.12 to v0.19 logical migration

This is a bounded, offline Compos v2-to-v3 migration. It is not an in-place
Rhiza upgrade or a general schema migration framework.

## Preserved state

The export contains all application tables, with explicit columns and exact
scalar values. The import preserves IDs, versions, sessions, memberships,
relations, schema revisions, sequence counters, generated values, command
receipts, Journal, and projection events. The v3 schema intentionally removes
`object_type_templates`. The export reports its row count but does not import
the retired table. Keep the source storage as the archive of that content.

Do not copy Rhiza QLog, SQL execution receipts, checkpoints, migration records,
applied slots, local materializations, or Graph/KV projection state. Rhiza v0.19
creates these under its own SQL policy 7. Graph and Meilisearch are derived views.

## Tools

`tools/rhiza-v012-export` is a separate Go module pinned to the source Rhiza
version. It opens an object-store read replica, not a voter. Each query and the
final sync must describe the same fully recovered source slot. A changing
source causes a retry or failure. The output file is exclusive and private.

`cmd/storage-import` uses the current Rhiza runtime and schema. It accepts only
the documented export and exact table/column inventory. The target must have
no application rows. Inserts keep normal foreign keys and triggers enabled.
Each request stays within Rhiza's SQL statement and byte limits. Verification
compares every row, not just row counts. `--verify-only` checks a recovered
target without inserting application rows.

Only Rhiza `commit_unknown` retries the exact batch, in the same running target,
with the same request ID and statements, at most three times. Rhiza verifies the
cached receipt's `before-ack` durability before confirming success. The importer
does not advance on an uncertain outcome or retry rejected SQL. A failed import
is not resumable by restarting against a partly populated target.

An instance can retain an older schema revision after its type evolves. The
import must preserve that historical pin. For such a row, one transaction sets
the parent type's schema version to the pinned revision, inserts the unchanged
instance, then restores the parent's original version. These statements are an
indivisible request group. Foreign keys and triggers remain enabled. The target
has no API or scheduler during import, and the final full comparison confirms
that all type and instance values equal the source.

The Dockerfile `storage-import` target runs only the offline command. It does
not start the HTTP server, workflow scheduler, or Conductor dispatcher.

## Cutover gates

1. Record the source image, deployment configuration, object namespace, and
   replica count. Keep credentials out of logs and the repository.
2. Build the helper images with the existing local or cluster-native BuildKit.
   Never use Google Cloud Build.
3. Export a preview while the source is serving. Validate the source identity
   and complete catalog. Inspect only safe aggregate counts in operator output.
4. Import into a fresh, isolated cluster ID, node identity, object prefix, and
   data directory. Do not connect the production Service or start a scheduler.
5. Check all table values. Stop the importer, recover into a new empty local
   directory from GCS, then repeat the full comparison.
6. Check in-flight Function/Agent executions. Do not lose an outstanding
   callback. Stop the source writer and SearchSyncer before the final export.
7. Export the quiescent source. Import and verify that exact final file into a
   fresh final namespace. If it is byte-identical to the fully verified rehearsal
   export, promote that recovered target instead. Stop its offline helper before
   starting the server with the same target identity. Keep the source namespace
   and private export.
8. Start the new server only after verification. Preserve authentication,
   workspace IDs, and service routing. Rebuild derived views. Resume SearchSyncer
   with the matching image. Check readiness, health, and observable API reads.

If import fails, do not expose its partial target. Preserve the source and use
a different fresh target for another attempt. Before new business writes, the
old image and old namespace can restore the source. After a new write commits,
image downgrade or routing to the old namespace loses new state and is not a
rollback. Keep the old namespace as an archive, not a second writer.

## Verification record

Executed on tg900 (`gke_tg900-cluster`, namespace `compos-conductor`) on
2026-10-08:

- Source: Rhiza v0.12.0, Compos schema v2, cluster `compos`, node `compos-0`,
  empty object prefix, stable recovered source slot **941**.
- Source had no pending Function/Agent callbacks. Its four Works were closed.
  The retired `object_type_templates` table had zero rows.
- All **30 application tables / 492 rows** matched every value after import
  and again after deleting the helper Pod and recovering into an empty local
  directory from GCS. IDs, versions, schema pins, sessions, memberships,
  relations, counters, generated values, ledger, and Journal were preserved.
- Application-table digest:
  `sha256:8e86f2df95a19ac843ba99a9ab0738c9d8bb955fe678a81f5a9300447f9a034b`.
- The source Compos and SearchSyncer were stopped before the final export.
  Preview and final export were byte-identical, SHA-256
  `b45ecb11ab9517ef78b1c755ad8f88eb5c1d9aad15d3d2c433bd2b4cfaf2fff4`.
  The verified, cold-recovered target was promoted without another import.
- Live cluster `v019-20261008-rehearsal3`, node `compos-v019-rehearsal3-0`,
  prefix `compos`, unchanged bucket `compos-prod-602454948273-20260815`.
  **The `rehearsal3` namespace is production; it is not disposable test data.**
- Migration-tool source commit `2d9e8b4c4cc2b50b06d3a6e459e56db89d22f369`
  passed Go tests, vet, CLI/server E2E, and both hosted
  historical CI run 37654758949 (original link removed).
  The existing tg900 BuildKit published the images; no Cloud Build was used.
- Initial cutover used images from that migration commit. Subsequent live
  search verification found missing scoped Meilisearch keys and a rejected
  canonical object ID containing `/`. Keys were reprovisioned with the same
  index-only permissions; no master key was assigned to Compos or SearchSyncer.
  Commit `e223565cf0ce271eaa0ed8df7a7cfb2bd732ea30` maps object projection IDs to
  workspace-qualified hashes and preserves canonical IDs in API search hits.
  Snapshot, create/update, move, and delete share that mapping. Full Go tests,
  vet, CLI/server E2E, and real-engine isolated Meilisearch scenarios passed.
  Its hosted historical CI run 37657577686 (original link removed)
  also completed successfully before the final image rollout.
- Final server image digest:
  `sha256:9fd68cc7cd6aa41463e97eff81e87c644ae0d3304f0bc4ca43bd7147645776f9`.
  Final SearchSyncer image digest:
  `sha256:d46677183899eca0e9338c6c2763ca8f7a3c5134a52e1c5725a44d6446f49fbc`.
- The old source objects remain unchanged. Private preview/final exports and
  original Deployment configurations are retained under
  `compos/migration-archives/20261008-v019/` in the existing private bucket.
  No snapshot contents or credentials are committed to this repository.
- Production Compos recovered from GCS, became Ready, and exposed the same
  workspace with a projection snapshot of 13 objects at sequence 35.
  The empty Knowledge graph rebuilt once and reported healthy projection v3.
- Final Compos, SearchSyncer, and unchanged Conductor were each 1/1 Ready with
  zero Pod restarts. SearchSyncer indexed all 13 objects, then entered event
  mode at sequence 35 without further sync errors. Search-key queries returned
  all 13 canonical IDs and versions, exactly matching the source and confined
  to the original workspace. Writer search/other-index access and search-key
  administration were rejected with 403. An authenticated read from Compos to
  Conductor succeeded and returned its two handlers. No production business
  records were created for verification.
- Three temporary export/import Deployments and the isolated real-engine
  test index/key were removed. The promoted target, old source, private exports,
  and original Deployment backups were retained.

Single-node GCS recovery does not prove multi-node HA or mixed-version support.
