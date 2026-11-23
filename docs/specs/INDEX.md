# Specification Index

This directory contains the normative greenfield Compos design. Product meaning
and architectural boundaries are governed by the constitutional architecture;
the specifications describe the currently implemented runtime contract.

## Normative Documents

Read in this order:

1. [Constitutional Architecture](../architecture/constitutional-architecture.md)
2. [README.md](../../README.md)
3. [Embedded Rhiza Runtime ADR](../adr/0003-embedded-rhiza-sql-ha.md)
4. [Go SQLite Support ADR](../adr/0002-go-sqlite-runtime.md)
5. [runtime-topology.md](runtime-topology.md)
6. [kernel-command-contract.md](kernel-command-contract.md)
7. [command-registry.md](command-registry.md)
8. [canonical-storage-schema.md](canonical-storage-schema.md)
9. [api-surface.md](api-surface.md)
10. [reactive-rule-adapter-boundary.md](reactive-rule-adapter-boundary.md)
11. [storage-ddl.md](storage-ddl.md)
12. [http-api-contract.md](http-api-contract.md)

Knowledge content and lifecycle are externally owned. Generic Ontology specs remain supported.

The [S2 removal decision](../adr/0004-s2-and-rhiza-boundaries.md) records why the
remote audit feature was removed while local audit and runtime delivery remain.

## Removed Legacy Direction

The PocketBase-era `2026-03-*` plans and specs have been removed from the
greenfield tree. They are not normative for this repository.

## Authority Rule

If product vocabulary or conceptual responsibility conflicts, the constitutional
architecture wins. For the currently implemented canonical write path, the Go
Kernel command contract wins:

```text
command intent -> Go Kernel validation -> canonical SQL commit
```

No journal, adapter, automation, board, S2 stream, or external quorum service may
bypass the Kernel to mutate business state. Rhiza executes the Kernel's validated
canonical SQL commits. Single-node object-storage recovery does not establish
multi-node HA.
