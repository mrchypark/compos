# ADR 0003: Embedded Rhiza SQL/Graph/KV HA

## Status

Accepted for the Go/Rhiza feature branch. It is not a statement that this
runtime has merged to the repository default branch or completed multi-node
production acceptance.

## Decision

Compos may open `github.com/mrchypark/rhiza` directly inside each Go server
process. Rhiza is a package, not a separately deployed database server; its
public HTTP adapter is not started. A fixed Rhiza quorum owns canonical SQL
commit order and each Compos process reads its node-local SQLite
materialization.

SQLite mode keeps the interactive transaction handlers. Rhiza mode plans each
Kernel command from linearizable reads, reasserts every consumed condition in
one atomic `Statements` command, and commits only through the quorum. Runtime
support writes such as workspace provisioning, callback ingress, outbox status,
and authentication bindings use the same replicated statement boundary.

`COMPOS_RHIZA_DATA_DIR` enables this mode. Cluster identity, node identity,
private peer address, fixed members, and S3-compatible shared object-store
settings use the `COMPOS_RHIZA_*` environment variables defined by
`cmd/server`. `COMPOS_RHIZA_MEMBERS` is the JSON encoding of `[]rhiza.Member`
without peer secrets. `COMPOS_RHIZA_PEER_TOKENS` supplies private tokens keyed
by node ID; Compos derives each member public key with `rhiza.PeerPublicKey`.
Multi-node clusters require shared object storage and peer tokens.

Rhiza v0.27.3 materializes SQL, Graph, and KV together in one embedded runtime.
Compos does not start a second Rhiza process or select a build profile. The
Knowledge reconciler and Concept graph APIs described in the original design
were subsequently removed when Knowledge ownership moved externally. Existing
graph data is retained; Compos starts no dedicated graph projection worker.

Direct SQLite backup is disabled in Rhiza mode. Rhiza checkpoints and the
shared object store own recovery. The S3-backed integration gate starts three
embedded nodes, stops one, and requires the remaining quorum to commit.
Rhiza v0.27.3 includes object-storage checkpoints, certified QLog compaction,
archive/checkpoint garbage collection, and anti-entropy. It does not recreate a
registered voter's missing local identity. The S3-backed gate also races two quorum writers on one unique key,
stops a voter, commits with the remaining quorum, then verifies that a new empty
directory is rejected after remote voter registration. It restarts the original
data directory, requires catch-up, and performs a write. Rhiza deliberately does
not recover a registered voter's missing local identity from object storage, so
this is not a PVC-free Pod-replacement/recovery design. The checked-in tg900
deployment uses one Rhiza node with `emptyDir` and GCS `before-ack`, so it
demonstrates remote recovery durability rather than HA. Multi-node failover and
upgrade must pass their integration gates before they are called supported
production operations. Legacy Compos schema migration, recovery certificate
signing/key rotation, and peer mTLS remain outside the contract.

SQLite-only mode remains available for deterministic local tests and
single-node development. It is not SQL HA.
