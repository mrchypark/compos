# tg900 GCS reset and Rhiza v0.12.0 verification

Verified 2026-09-05. User explicitly approved resetting the current Compos
workspace, stopping the two legacy writers, clearing the existing bucket and
then upgrading Rhiza. No new bucket, PVC or Cloud Build was used.

## Applied state

- Context: `gke_patch2-the-new-era_asia-northeast3_tg900-cluster`
- Namespace: `compos-conductor`
- Existing bucket: `compos-prod-602454948273-20260815`
- Object keys: `compos/archive/...` and `compos/checkpoint/...`
- Cluster/node: `compos` / `compos-0`; object prefix empty
- Provider/durability: `gcs` / `before-ack`
- KSA: `compos-rhiza-gcs`; direct GKE WIF, bucket-scoped
  `roles/storage.objectUser`, no JSON key
- Rhiza: `v0.12.0`; LatticeDB Go binding: `v0.5.1`
- Server image: `registry.example.invalid/compos/compos-server@sha256:971322b9a49141e3c2dd84fe6e0c0f42f1442f76416d4fbd78cbf8fc6bed7105`
- Build/push: existing Kubernetes `tg900-buildkit`, target `server`, linux/amd64

Deploy stopped legacy StatefulSet `compos/compos`, Deployment
`compos/compos-s2-lite` and their sole managing `compos/compos-operator`.
All had desired/ready 0 and no writer pods before bucket cleanup. Deploy
reported 75 seconds without object recreation, and the parent independently
confirmed the bucket was empty before starting new Compos. Bucket and its
soft-delete policy were preserved. Probe objects and the probe Pod were removed.
The current bucket objects are new Rhiza data: do not repeat the cleanup.

## Verification

1. Deploy's KSA create/get/list/delete GCS probe passed; parent verified IAM.
2. Existing v0.11.0 server started a fresh workspace with GCS configured.
3. Replacing its Pod recovered the same workspace and projection data hash.
4. `go get github.com/mrchypark/rhiza@v0.12.0`, `go mod tidy`, `go test ./...`,
   `go vet ./...`, and `git diff --check` passed.
5. v0.12.0 image recovered the v0.11.0 GCS data, including a published
   checkpoint. Another v0.12.0 Pod replacement preserved the same result.
6. Health returned `status=ok`, storage `kind=rhiza`, `cluster_id=compos`,
   `ready=true`. Compos, Conductor and SearchSyncer were all Ready 1/1.
7. Only the dedicated `compos-tg900-objects` search index was cleared and
   rebuilt. Search returned exactly the new workspace's two objects, with no
   old workspace documents; Syncer returned to event polling at sequence 0.

All three recovery comparisons matched:

- Workspace: `wks_9692eaf5-fa22-48f8-a459-f31889eb0bb0`
- Objects: 2; Knowledge: 0; projection sequence: 0
- Data hash: `sha256:d402bef01ef08856ddb898e693447468007fb5329e7064705099d24b969ee15e`

## Boundaries

- Recovery verification covers this fresh bootstrap dataset, not arbitrary
  historical graph formats, concurrent HA failure, or a real-user login E2E.
- patch-data accounts, Meilisearch service, legacy search index and Conductor
  image were not reset. Compos memberships from the old workspace were reset;
  normal verified login enrolls users into the new workspace.
- Legacy writer/operator shutdown and bucket IAM are live changes coordinated
  with Deploy. Its dirty-main Git gate still prevents declarative reconciliation;
  do not re-enable the legacy stack while resolving that separate conflict.
- Meilisearch still has its previously documented ephemeral index/key lifecycle.
- Source edits remain uncommitted; this request did not authorize Git commit/push.
