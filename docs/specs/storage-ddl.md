# Storage DDL Reference

The canonical DDL source of truth is
[`internal/store/schema.sql`](../../internal/store/schema.sql). It is embedded
by the Go store package and bootstraps fresh canonical SQL storage: Rhiza in
deployed mode and SQLite in local/test mode.

This is a breaking, fresh-only baseline. A database must be empty or carry the
exact `schema_metadata.identity` value; legacy, partial and foreign databases are rejected
without mutation and must be recreated explicitly.

This file intentionally does not duplicate the SQL. Inspect the embedded source
or a freshly created database when schema reference output is needed.
