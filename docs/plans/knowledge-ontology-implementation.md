# External Knowledge ownership

Knowledge content and its accept/remove/propose/review lifecycle are owned by an
external system. Compos no longer parses OKF, serves Knowledge/grounding/traversal
APIs, or maintains a dedicated graph projection.

Generic Ontology `spec.publish` and Task `ontology_context` remain supported.
Opening an existing v3 database atomically drops only `ontology_groundings`,
`knowledge_relations` and `knowledge_documents`, then advances identity to v4.
Their contents are intentionally deleted. Fresh databases omit these tables.
Shared Journal kinds, command receipts, events and Ontology revisions remain;
existing graph artifacts are left untouched. Logical exports exclude the retired
tables with row counts; imports reject old exports that still include them.

Search snapshots use schema version 3 and contain objects only. The search-syncer
clears and rebuilds its active workspace on each restart, removing stale Knowledge
documents. Retained legacy events are ignored while their cursor advances.
The search API independently removes hits without a non-empty string `object_id`
to hide stale Knowledge results during upgrades. No new filterable attribute or
syncer restart is required for availability. It reports `returnedHits` for the
filtered page and omits engine totals and facets. Until the syncer rebuilds the
workspace, pages may be short or empty and offsets refer to engine results.
