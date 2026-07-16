# Additional Performance Datasets

## Status

This document is a design proposal for future work. It does not describe a
feature currently implemented by the scraper.

## Objective

Allow users to add performance datasets without modifying or rebuilding the
scraper. A dataset consists of:

- one or more Oracle queries;
- an explicitly typed PostgreSQL destination table;
- a collection interval and query timeout;
- optional indexes and collection limits;
- the standard partitioning and retention behavior provided by the scraper.

These should be called **additional performance datasets** or **declarative
performance collectors**, rather than metrics. Unlike a conventional metric,
each collected row may contain SQL identifiers, session details, objects,
users, events, or other high-cardinality diagnostic data.

The existing native collectors should remain implemented in Go. DAH, SQL
performance, current sessions, and blocking sessions contain specialized
collection, conversion, and persistence logic that should not be weakened to
fit a generic abstraction.

## Recommended Approach

Use declarative, typed dataset definitions with controlled table creation. The
scraper owns the PostgreSQL schema generated from each declaration. Users
provide table metadata and Oracle `SELECT` statements, but do not provide
arbitrary PostgreSQL DDL.

An initial configuration could resemble:

```yaml
performanceDatasets:
  - name: temp_usage
    table: oracle_temp_usage_samples
    scrapeInterval: 30s
    queryTimeout: 10s
    query: |
      select inst_id,
             username,
             sql_id,
             tablespace,
             blocks * 8192 as bytes_used
        from gv$sort_usage
    columns:
      - name: inst_id
        type: integer
        nullable: false
      - name: username
        type: text
      - name: sql_id
        type: text
      - name: tablespace
        type: text
      - name: bytes_used
        type: bigint
    indexes:
      - [source_database, username, collected_at]
      - [source_database, sql_id, collected_at]
    maxRowsPerScrape: 10000
```

The exact configuration format may change during implementation. If multiple
queries must populate the same table, the model should separate table metadata
from its collectors, for example:

```yaml
performanceDatasets:
  - name: object_activity
    table: oracle_object_activity_samples
    columns:
      - name: object_owner
        type: text
      - name: object_name
        type: text
      - name: activity_type
        type: text
      - name: activity_value
        type: double_precision
    collectors:
      - name: segment_statistics
        scrapeInterval: 60s
        queryTimeout: 15s
        maxRowsPerScrape: 20000
        query: |
          select owner as object_owner,
                 object_name,
                 statistic_name as activity_type,
                 value as activity_value
            from gv$segment_statistics
```

All queries targeting one table must return a compatible column set. Whether
collectors may populate only a subset of nullable columns should be decided
explicitly before implementation; requiring the complete declared column set
would make the first version simpler and safer.

## Scraper-Managed Columns

The scraper should add and populate at least these columns automatically:

- `collected_at`: timestamp assigned to the collection batch;
- `source_database`: configured source database name.

Users should not redefine these columns. They provide consistent filtering,
partitioning, retention, and dashboard behavior across native and additional
datasets.

The need for other standard columns, such as an Oracle instance identifier,
should be decided per dataset. `inst_id` should not be injected automatically
because not every query or database topology provides it.

## PostgreSQL Table Management

For each dataset, the scraper should:

1. Validate the complete declaration before executing Oracle or PostgreSQL
   statements.
2. Create a partitioned parent table when it does not exist.
3. Partition by `collected_at`, following the same daily partition strategy as
   the native performance tables.
4. Create required partitions before inserting a batch.
5. Create only validated, declared indexes.
6. Insert batches efficiently, preferably through the existing PostgreSQL
   `COPY` path.
7. Apply the global retention policy by dropping expired partitions.

Dynamic SQL identifiers must be quoted through the PostgreSQL driver's
identifier API, such as `pgx.Identifier`. Table names, column names, and index
columns must be validated independently of quoting. Values must always use
parameters or `COPY`; they must never be concatenated into SQL.

## Supported Types

Column types must be explicit. The scraper should expose a small allowlist of
portable PostgreSQL types rather than accepting arbitrary type expressions. A
reasonable initial set is:

- `text`
- `integer`
- `bigint`
- `double_precision`
- `numeric`
- `boolean`
- `timestamp_with_time_zone`
- `timestamp_without_time_zone`

Aliases in configuration may map to canonical PostgreSQL types. For example,
`timestamptz` could map to `timestamp with time zone`, but arbitrary modifiers
or user-provided type SQL should not be accepted initially.

Explicit types are preferable to inferring PostgreSQL types from Oracle result
metadata. Oracle drivers can report values differently depending on query
expressions, precision, scale, null results, and driver implementation. Type
inference would make schema creation and behavior less predictable across the
`godror` and `go-ora` builds.

The collector must convert every Oracle value to its declared destination type
and produce a useful error containing the dataset, query, column, source type,
and destination type when conversion fails.

## Schema Compatibility and Evolution

Automatic schema evolution must be conservative:

- Creating a missing table is allowed.
- Adding a new nullable column may be allowed.
- Adding a non-nullable column without a safe default should fail.
- Changing a column type should fail and require a manual migration.
- Removing or renaming a column should fail and require a manual migration.
- Changing partition keys or managed columns should fail.
- Index additions may be applied automatically after validation.
- Index removals should initially require manual administration.

At startup, the scraper should compare each declaration with the PostgreSQL
catalog. An incompatible schema must disable that dataset with a precise error;
it must not silently modify or discard existing data. Other native and
additional collectors should continue when isolation is possible.

A schema fingerprint stored in a small metadata table may simplify compatibility
checks, but the PostgreSQL catalog must remain the authoritative description of
the physical table.

## Collection Pipeline

The existing performance collection model can be extended with a generic batch
type, conceptually:

```go
type DatasetBatch struct {
    DatasetName   string
    TableName     string
    SourceDatabase string
    CollectedAt   time.Time
    Columns       []string
    Rows          [][]any
}
```

`PerformanceSamples` could carry `[]DatasetBatch`, or generic datasets could use
a separate channel and persistence worker. A separate channel is likely cleaner
if datasets have independent intervals, limits, or failure behavior. The final
choice should follow the current collector and writer lifecycle after it is
reviewed during implementation.

The important guarantees are:

- a slow additional query does not block native performance collectors;
- one invalid or failing dataset does not stop unrelated datasets;
- shutdown and cancellation propagate to every query and writer;
- a partially converted batch is not inserted;
- retries do not create uncontrolled duplicate rows;
- reload behavior is explicit and observable.

## Required Guardrails

Declarative collectors execute user-defined Oracle queries and can generate
large PostgreSQL datasets. The implementation needs mandatory controls:

- `queryTimeout` for every collector;
- `scrapeInterval` with a documented minimum;
- `maxRowsPerScrape`;
- a maximum batch size in bytes;
- a maximum number of configured datasets and collectors;
- a maximum allowed number of columns and indexes;
- bounded queues between collection and persistence;
- cancellation when the previous execution is still running;
- clear handling of overlapping collection intervals;
- validation that every index references declared or managed columns;
- rejection of reserved managed column names;
- rejection of duplicate table, dataset, collector, and column names;
- read-only Oracle credentials as an operational requirement;
- logging and internal health counters for rows collected, rows rejected,
  duration, timeouts, conversion failures, and persistence failures.

The first version should skip a collection when the previous execution for the
same collector is still active. Starting concurrent copies of the same expensive
Oracle query can amplify database load during an incident.

Row and byte limits are both necessary. A row count alone does not constrain
large text values, and a byte limit alone is harder for administrators to reason
about when tuning a query.

## Query Validation and Security

Configuration should permit only a single Oracle query per collector. It should
reject obvious non-query statements and multiple statements. This check is a
guardrail, not a complete SQL security boundary; Oracle permissions remain the
actual authority.

The scraper cannot reliably prove that arbitrary Oracle SQL is inexpensive or
free of side effects. Administrators must review additional collectors and use
a dedicated account with the minimum required grants. Query timeout and row
limits do not prevent a bad execution plan from consuming substantial Oracle
resources before cancellation.

PostgreSQL table and column creation should be performed by a role with only the
required privileges. If production policy separates schema ownership from data
writing, an optional validation-only mode may be needed so administrators can
pre-create tables while the runtime role receives only partition and insert
privileges.

## Configuration Reload

Hot reload is useful but introduces schema and scheduling complexity. A safe
policy would be:

- new compatible datasets start after validation and table preparation;
- query, timeout, interval, and limits may change dynamically;
- additive compatible columns may be applied before collection resumes;
- incompatible schema changes are rejected while the previous valid definition
  continues running;
- removing a definition stops collection but never drops its table or data;
- table deletion is always an explicit database administration operation.

The log must distinguish between the loaded configuration, the last valid active
configuration, and any rejected replacement.

## Retention and Partitioning Caveats

Additional datasets should participate in the same global retention setting as
native performance tables. This preserves the current all-in-one retention
model and avoids a large configuration surface.

However, users must understand that retention by `collected_at` removes complete
time partitions. It cannot selectively preserve rows within a partition, and a
dataset introduced later may have a different oldest available timestamp than
native datasets.

Indexes consume disk and increase insert cost. The scraper should create only
declared indexes and should not guess indexes from the query or dashboards.
Documentation should recommend starting with the partition timestamp and the
few dimensions actually used to filter investigations.

PostgreSQL avoids the Prometheus time-series cardinality model, but it does not
make high-cardinality data free. SQL IDs, users, sessions, objects, modules, and
events still increase row counts, index sizes, storage, vacuum work, and query
cost. Every proposed dataset needs a useful diagnostic purpose, bounded
collection, appropriate indexes, and retention.

## Failure Behavior

Failures must be isolated and visible:

- Oracle query errors fail only the affected collection attempt.
- Conversion errors reject the complete batch and identify the bad column.
- PostgreSQL insertion errors retain no partial batch when transactions permit.
- Schema incompatibility disables only the affected dataset.
- Partition creation failures prevent insertion and report the target table and
  partition boundary.
- Repeated failures should be rate-limited in logs without hiding current health
  state.

Native collectors must continue operating when an additional dataset fails.
There should be no global scraper failure solely because an optional dataset is
invalid, unless strict startup validation is explicitly requested in the future.

## Testing Requirements

Implementation should include:

- strict configuration parsing and validation tests;
- identifier and reserved-name tests;
- tests for every supported type and both Oracle drivers;
- nullability and conversion failure tests;
- table and partition creation integration tests;
- schema compatibility and additive evolution tests;
- `COPY` tests for dynamic column lists;
- timeout, cancellation, overlapping execution, and bounded queue tests;
- row-limit and byte-limit tests;
- reload tests for additions, removals, valid changes, and rejected changes;
- retention tests proving additional dataset partitions are removed;
- failure-isolation tests proving native collectors continue;
- Graphviz updates for the new Oracle-to-PostgreSQL data flow.

## Hard Parts

The difficult part is not executing a query or creating a table. The hard parts
are maintaining predictable behavior over time:

1. **Cross-driver type conversion.** `godror` and `go-ora` may expose Oracle
   values differently, especially numbers, timestamps, LOBs, and nulls.
2. **Schema compatibility.** Configuration changes must never silently corrupt,
   reinterpret, or discard existing data.
3. **Dynamic table and partition management.** Identifiers, privileges,
   transactions, concurrent creation, and retention all need careful handling.
4. **Resource isolation.** A user query can be slow, return too much data, or
   block writers; optional datasets must not degrade native collection.
5. **Useful diagnostics.** Dynamic datasets make errors harder to understand,
   so messages and health signals must identify the exact dataset, collector,
   query, column, and stage.
6. **Reload semantics.** Runtime changes combine scheduler, schema, and query
   lifecycle concerns and must preserve the last valid state.
7. **Operational ownership.** Some environments permit runtime DDL while others
   require tables and indexes to be created through a controlled deployment.

The overall difficulty is moderate for a prototype and substantial for a
production-quality feature. A prototype that supports a few scalar types and
creates one table can be built relatively quickly. A safe implementation with
both Oracle drivers, schema evolution, isolation, retention, reload, and strong
diagnostics is a major feature and should be delivered incrementally.

## Suggested Delivery Phases

### Phase 1: Static Typed Datasets

- Load definitions only at startup.
- Support one query per dataset.
- Support a small scalar type allowlist.
- Create partitioned tables and daily partitions.
- Insert through dynamic `COPY`.
- Enforce timeout, row count, and batch byte limits.
- Apply global retention.
- Reject all schema differences except creation of a missing table.

### Phase 2: Multiple Collectors and Compatibility

- Allow multiple compatible queries to populate one table.
- Add catalog-based schema validation.
- Allow additive nullable columns and index additions.
- Improve per-dataset health reporting and failure isolation.

### Phase 3: Safe Reload and Operational Modes

- Reload compatible definitions without restarting.
- Preserve the last valid definition after a rejected reload.
- Add validation-only or schema-plan output for environments where runtime DDL
  is prohibited.
- Add tooling or commands to inspect the effective dataset schema.

The future Temp/PGA and object-hotspot dashboards are good candidates for using
this feature after Phase 1 proves reliable. They would also provide realistic
requirements and test data without replacing the specialized native collectors.

## Rejected Alternative: Arbitrary DDL

Allowing users to provide a PostgreSQL `CREATE TABLE` statement together with
an Oracle query would be easier to prototype, but it is far less effective and
unsafe as a product design. The scraper could not reliably validate column
mapping, partitioning, indexes, retention compatibility, identifier safety,
schema evolution, or driver conversions. It would also require excessive
PostgreSQL privileges and produce installations that are difficult to support.

For these reasons, arbitrary DDL should not be implemented. Controlled table
creation from an explicit typed declaration provides enough flexibility while
keeping collection, persistence, retention, and upgrades within a supportable
contract.
