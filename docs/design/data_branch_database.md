# Database data-branch operations

```sql
DATA BRANCH DIFF DATABASE source_db{snapshot='source_snapshot'}
  AGAINST destination_db
  [TABLES (t1, t2) | IGNORE TABLES (metadata_table)]
  OUTPUT SUMMARY;

DATA BRANCH MERGE DATABASE source_db
  INTO destination_db
  [TABLES (t1, t2) | IGNORE TABLES (metadata_table)]
  [WHEN CONFLICT FAIL | SKIP | ACCEPT]
  OUTPUT SUMMARY;
```

DIFF accepts current or historical databases on either side. MERGE accepts
a historical source and requires a current destination. Each command returns
one row per selected ordinary table, sorted by table name:

`table_name`, `source_exists`, `destination_exists`, `source_inserted`,
`source_deleted`, `source_updated`, `destination_inserted`,
`destination_deleted`, `destination_updated`, `conflicts`.

The counters have the same logical, pre-merge meaning as table-level
`MERGE ... OUTPUT SUMMARY`. Equal changes are excluded. Conflict counts do
not depend on SKIP/ACCEPT. Existence flags distinguish an empty table from
a missing table. A table present only on one side of DIFF contributes that
side's row count as inserts. Empty databases return an empty result set with
the same columns. Internal index tables and non-table objects are excluded.
Snapshot enumeration includes tables dropped since the snapshot and validates
the snapshot's database scope.

The default table set is the union of both endpoints. `TABLES` selects exact
names and rejects unknown or duplicate names; `IGNORE TABLES` removes exact
names from the union. The clauses cannot be combined. Applications can use
these clauses to exclude their own commit metadata.

MERGE handles data. Selected tables must exist on both sides; use schema DDL
or cloning to reconcile added tables before MERGE. Existing table-level column
compatibility and conflict semantics apply. It does not infer schema changes
or delete target-only tables. FAIL is the default conflict policy.

Enumeration, privilege checks and all selected tables use one owner transaction.
All endpoint SELECT privileges and destination INSERT/UPDATE/DELETE privileges
are checked before any data is applied. In autocommit mode an error on any
table rolls back the whole database statement. Explicit BEGIN transactions
retain control of commit/rollback. Implicit autocommit=0 mutations are rejected
like table-level MERGE. Intermediate per-table results are never sent to the
client. Schema reconciliation performed by a caller before this statement is
outside its transaction unless the caller supplies a supported explicit one.

Database operations reuse the native table diff/merge workers. A merge still
performs one HashDiff per selected table; the interface removes per-table
client round trips, not the underlying per-table work.
