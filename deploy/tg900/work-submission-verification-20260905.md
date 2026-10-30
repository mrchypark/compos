# Atomic Work submission release — 2026-09-05

Source: `82b42b1` on `feature/rhiza-v0.9.1-port`, pushed to origin.
Built and pushed with the existing Kubernetes `tg900-buildkit` builder,
`linux/amd64`, Dockerfile targets `server` and `search-syncer`. No Cloud Build.

Deployed to context `gke_patch2-the-new-era_asia-northeast3_tg900-cluster`,
namespace `compos-conductor`, by changing only Deployment container images:

| Image | Deployed digest |
| --- | --- |
| compos-server | `sha256:d05c077c5c72a5f1f3abe9a5c196ad3417959c79b022f7766b898f31e5a35ed6` |
| compos-search-syncer | `sha256:60881dd6b7a8d6e4472ba307f8db382d2cca7a65e8afa2091ee3936ed0672751` |

Both images are under `registry.example.invalid/compos`
and tagged `tg900-work-changes-82b42b1`. Pod image IDs match these digests.
Conductor was not replaced. Auth, GCS bucket/prefix/cluster/node identity,
before-ack durability, service accounts, secrets and emptyDir volumes remain
unchanged. No PVC, bucket creation or data reset was performed.

## Functional checks before release

- `go test ./... -count=1` and `go vet ./...` passed.
- `go test -race ./internal/kernel ./internal/server -count=1` passed.
- SQLite and real embedded Rhiza: existing customer + new quote + three new
  items, forward references, automatic numbers, resolved submission, durable
  receipt mappings and identical retry convergence passed.
- Concurrent Rhiza submissions produced distinct quote/item numbers.
- Late constraint failure rolls back objects, relations, counters, submission,
  receipt and projection effects. Unknown/cyclic references are rejected.
- New-to-new `relation.link` with submission-local references passed on both
  stores, including injected endpoint version preconditions and duplicate retry.
- Exact-count guard compaction and the native 64-statement rejection are tested;
  oversized transactions are never split.

## Live checks at 14:17 UTC

- Compos generation 10/observed 10 and Syncer 5/observed 5: each desired,
  updated, ready and available replicas = 1. Rollouts completed successfully.
- Compos, Syncer and unchanged Conductor pods Ready, zero restarts after more
  than two minutes; Compos and Conductor EndpointSlices ready.
- Compos health: `status=ok`, Rhiza `ready=true`, stable `cluster_id=compos`.
- Same bootstrap workspace recovered:
  `wks_9692eaf5-fa22-48f8-a459-f31889eb0bb0`.
- Syncer rebuilt a snapshot of four existing objects at sequence 2, then
  resumed event polling with no errors. Meilisearch health `available`.
- Compos-to-Conductor authenticated `GET /v1/handlers`: HTTP 200.
  Conductor does not expose `/health`; readiness uses its existing TCP probe.
- Unauthenticated definition reads and Kernel POST: HTTP 401.
- Legacy Compos StatefulSet, s2-lite and operator remain scaled to zero.

The QUIC receive-buffer warning remains non-fatal. Knowledge health still
reports `production_approved=false`; this release does not claim to approve the
separate graph production gate.

No ordinary user session was available to this task. Authenticated live Work
mutation and a new live Conductor execution were **not** run. Atomic business
behavior was verified locally against both real storage implementations;
Backoffice integration must use its normal patch-data/Compos session.

## Backoffice connection

In-cluster base URL: `http://compos.compos-conductor.svc.cluster.local:7878`.
No public ingress was created. Local access can use:

```sh
kubectl --context gke_patch2-the-new-era_asia-northeast3_tg900-cluster \
  -n compos-conductor port-forward svc/compos 7878:7878
```

Use [the HTTP contract](../../docs/specs/http-api-contract.md), including the
Backoffice business processing and submission-local reference sections.
Exchange a normal patch-data token at `POST /api/v1/auth/pocketbase`, then use
the returned Compos session bearer. Submit one `work.submit` command through
`POST /api/v1/kernel/commands`; retain the original command and idempotency key
when a transport result is uncertain. Do not split saves around submission.

Rollback image pins, if needed (no data deletion):

- Server: `sha256:971322b9a49141e3c2dd84fe6e0c0f42f1442f76416d4fbd78cbf8fc6bed7105`
- Syncer: `sha256:da28ad46f135ec27898450fb1e26e151c85e3817d41547127fe1734ab7482c5b`
