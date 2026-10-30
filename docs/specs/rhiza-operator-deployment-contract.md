# Rhiza Operator Deployment Contract

## Ownership

The Go server embeds Rhiza. Kubernetes does not deploy a database sidecar or a
separate database service. The Rust Operator owns only placement, stable
membership inputs, disposable local materialization, Secrets, Services and readiness
status. Rhiza owns replicated SQL/Graph/KV ordering, checkpoints, log retention,
bootstrap and peer catch-up.

## Required topology

- `spec.replicas` is odd and at least three.
- Pod names are stable Rhiza node IDs.
- The headless Service publishes every pod before readiness and exposes the
  private Rhiza peer port over UDP.
- `COMPOS_RHIZA_MEMBERS` is rendered from StatefulSet ordinal DNS names.
- `spec.server.envSecret` supplies `COMPOS_RHIZA_PEER_TOKENS`, a JSON map from
  pod name to private peer token. Compos derives public keys; raw tokens are
  never placed in `COMPOS_RHIZA_MEMBERS`.
- Every pod has a disposable `emptyDir` mounted at `/data`; the product does not
  require a PVC for Rhiza data.
- Every member uses the same configured object-store bucket and prefix with
  `COMPOS_RHIZA_OBJECT_DURABILITY=before-ack`.
- The admin token and object-store credentials come from the deployment's
  configured Kubernetes credential mechanism.

Fixed membership changes are replacement operations. The current Operator does
not perform online membership reconfiguration.

`envSecret` is injected into every voter Pod. Consequently the peer-token map
has one shared cluster trust boundary: any voter can read every token in that
Secret. Omitting tokens from the membership JSON prevents accidental public
configuration disclosure; it does not provide per-node credential isolation.
Per-node secret delivery requires a separate Operator design and is not part of
this contract.

## Health and recovery

The server health response reports actual embedded Rhiza readiness as
`storage.ready`. The Operator reports StatefulSet ready replicas; it does not
claim quorum from Kubernetes pod counts and does not automatically select or
restore a backup.

A container restart retains its Pod's `emptyDir`, so Rhiza can catch up using
the original local voter identity. A deleted Pod has a new empty directory, and
Rhiza rejects it once that voter is remotely registered: checkpoints/logs do not
recreate voter identity or promises. The current Operator does not automate
fenced voter replacement, so multi-node `emptyDir` supports initial bootstrap
and in-Pod restart only, not Pod replacement or rolling upgrade. StatefulSet
startup is `Parallel` so an initial quorum can form; it must not use Kubernetes
`OrderedReady`. The integration gate requires an empty-directory replacement
rejection, restart from the original data directory, catch-up, and a post-restart
commit. This is not proof of a live multi-node operator rollout. tg900 currently
runs one node with `emptyDir` and GCS `before-ack`, validating remote durability
only.

Changing an existing StatefulSet's pod-management or volume-claim template is
not an in-place upgrade contract. Plan an explicit replacement with the same
validated Rhiza identity and recoverable remote state; never delete old PVC data
as part of merely applying this new manifest. The current operator object-store
rendering uses endpoint/access-key Secrets (S3-compatible); tg900's direct GCS
Workload Identity Deployment remains a separate manifest.

Rhiza v0.19.0 is also not an in-place data upgrade. It requires SQL execution
policy 7 and rejects earlier SQL
policy materializations, checkpoints, history, and WAL artifacts. Do not point
a v0.19.0 StatefulSet at an existing pre-v0.19 data directory or object-store
prefix. Quiesce and preserve the old installation, export verified application
state with the compatible runtime, then import into a fresh cluster ID, object
prefix, data directories, and peer identities before traffic cutover. The
Operator does not perform this logical migration.
