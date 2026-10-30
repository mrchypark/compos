# Compos Operator

The operator deploys Compos as an odd, fixed-membership StatefulSet using the
embedded Rhiza SQL/Graph/KV package. Rhiza is not a separate database service.
The product storage target is shared object storage, not a PVC.

The breaking `ComposCluster` contract contains only:

- `spec.replicas`: odd and at least three;
- `spec.server.image`: immutable published `compos-server` image reference;
- `spec.server.rhiza`: cluster ID, UDP peer port and admin-token Secret;
- `spec.objectStore`: shared S3-compatible checkpoint store;
- one disposable `emptyDir` materialization directory per pod.

The referenced Secrets provide the configured Rhiza and object-store credentials.
`spec.server.envSecret` must also contain `COMPOS_RHIZA_PEER_TOKENS`, a JSON map
of stable pod name to private peer token. The server derives public membership
keys from it; do not put peer tokens in `COMPOS_RHIZA_MEMBERS`.
Because `envSecret` is injected into every voter Pod, this is a shared
cluster-token trust boundary, not per-node secret isolation. A compromised voter
can read the complete map. Per-node credential delivery needs a separate
Operator design.
The operator renders fixed pod DNS membership, uses `Parallel` pod startup, and
forces object-store durability to `before-ack`. Provider-specific credential
delivery is deployment configuration, not a second storage authority.

The operator does not infer quorum or run destructive restore jobs. An in-Pod
container restart retains `emptyDir` and Rhiza can catch up from its original
local voter identity. A deleted Pod gets a new `emptyDir`; Rhiza intentionally
rejects that already-registered voter rather than reconstructing promises from
object storage. Status reports StatefulSet readiness only. Fenced voter
replacement or durable local storage is required before treating multi-node Pod
replacement or rolling upgrade as supported.

tg900 is separately deployed as one Rhiza node with `emptyDir` plus GCS
`before-ack` durability. It demonstrates recovery durability, not a live
multi-node operator rollout, failover, or upgrade; those remain integration
gates.

Rhiza v0.19.0 requires SQL execution policy 7 and rejects earlier
materializations, checkpoints, history, and WAL artifacts, including v0.18.0
policy-4 state. Do not replace an existing cluster image while
reusing its data directory or object-store prefix. Preserve and quiesce the old
runtime, export verified application state, then import it into a fresh cluster
ID, object prefix, data directory, and peer identities. The Operator does not
perform that logical migration.

Apply the CRD and controller chart, then create
`deploy/operator/compos-cluster.example.yaml`.

The same controller also owns the rebuildable Meilisearch projection. Apply
`deploy/operator/compos-search-syncer.example.yaml` after creating its Compos
session-token Secret. The chart installs the `ComposSearchSyncer` CRD and RBAC;
the controller creates Meilisearch, its generated master-key Secret, and the
`compos-search-syncer` Deployment. Set `spec.syncer.image` to the fully
qualified GAR tag printed by the `build-images` workflow for
`compos-search-syncer`; no mutable or local image default is assumed. Unless
`spec.meilisearch.masterKeySecretRef` is set, the controller generates a
per-CR `<name>-meilisearch` master-key Secret and garbage-collects it with the
CR. Generated Secrets are immutable. A configured Secret reference is used
as-is and is never owned or deleted by the controller; it must already exist,
contain the configured key, and be immutable.
Meilisearch is intentionally a single ephemeral replica. The syncer recreates
the index from the canonical snapshot after index loss; `status.phase=Running`
means both workloads are ready, not that every projection event is caught up.
