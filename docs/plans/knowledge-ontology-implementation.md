# Knowledge and Ontology Implementation

Status: implemented

## Canonical model

Canonical SQL is the Knowledge and Ontology store: Rhiza SQL in deployed mode
and SQLite only in local/test mode. Accepted Open Knowledge Format Markdown is
preserved exactly in `knowledge_documents`; parsed relations, groundings, review
evidence, and projection events are committed through the Go Kernel transaction
boundary. Full-text search is rebuilt in external Meilisearch.

The embedded Rhiza package provides SQL HA, Graph and KV in the Compos process.
Rhiza Graph contains only a rebuildable ontology reachability projection. Search
uses a rebuildable Meilisearch projection and path-preserving traversal remains
a canonical SQL read; bounded unique-node reachability uses Rhiza when its
cursor has reached the canonical SQL projection head.

## Implemented flow

1. `knowledge.propose` appends an immutable proposal fact.
2. `knowledge.review` requires a human principal and records the review.
3. An accepted proposal publishes an immutable Ontology spec revision.
4. `knowledge.accept` validates and stores the exact OKF source, parsed Concept,
   relation and grounding snapshots, and a projection event atomically.
   Its preserved source-byte hash is the immutable revision ID; the append-only command ledger
   retains prior accepted sources and removal tombstones without another history table.
5. The asynchronous Rhiza reconciler applies the corresponding graph snapshot,
   stores per-workspace cursors and digest, and rebuilds on mismatch.
6. Stale or unavailable graph state fails the bounded reachability endpoint closed;
   it never blocks canonical Knowledge writes.

## Storage and API

- `knowledge_documents`: exact source plus parsed current snapshot
- `command_ledger`: immutable accepted source revisions and removal tombstones
- `knowledge_relations`: Markdown and formal Ontology edges
- `ontology_groundings`: many-to-many Concept ↔ EntityDefinition mapping
- Meilisearch: rebuildable external full-text projection fed by projection events
- `spec_revisions(kind=ontology)`: immutable promoted Ontology definition
- `journal_records`: proposal and human review evidence
- `GET /api/v1/workspaces/{workspace_id}/knowledge`
- `GET /api/v1/workspaces/{workspace_id}/knowledge/{bundle_id}/{concept_id}`
- `GET /api/v1/workspaces/{workspace_id}/ontology/traverse`
- `GET /api/v1/workspaces/{workspace_id}/ontology/reachable-node-ids`
- `GET /api/v1/workspaces/{workspace_id}/ontology/groundings`
- `POST /api/v1/workspaces/{workspace_id}/search`

## Operational gates

- Multi-node Rhiza requires fixed membership, peer authentication and shared object
  storage with `before-ack` durability.
- Nodes may use disposable local `COMPOS_RHIZA_DATA_DIR` materialization when
  shared object-store recovery is configured. tg900 uses `emptyDir` plus GCS
  `before-ack`; this is durability/recovery validation, not multi-node HA.
- Health exposes actual embedded Rhiza readiness; it does not synthesize a quorum
  claim.
- The three-node integration test must prove commit with one stopped node, restart
  catch-up from the same data directory, and a successful post-restart commit.
