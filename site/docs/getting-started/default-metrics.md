---
title: Default Metrics
sidebar_position: 2
---

# Default Metrics

The scraper includes `default-metrics.toml` for low-to-medium-cardinality Oracle
database metrics. These metrics are collected on the configured schedule and
stored in PostgreSQL, not exposed as Prometheus text.

Generic TOML metrics are written to:

```text
oracle_metric_samples
```

Each stored metric sample includes:

- `collected_at`
- `source_database`
- `context`
- `metric_name`
- `metric_help`
- `metric_type`
- `value`
- `labels`

The old raw JSON copy of each Oracle query row is no longer stored.

## Included Metric Contexts

The default metrics file currently includes contexts such as:

- database type
- sessions
- resource limits
- ASM diskgroup usage
- activity counters
- process count
- wait time by wait class
- tablespace usage
- selected database parameters
- database platform
- top SQL from `GV$SQLSTATS`
- cache hit ratio from `GV$CON_SYSMETRIC`

These are useful for broad operational views and capacity checks. Detailed
SQL/session troubleshooting is handled by the dedicated performance samplers and
dashboards.

## Performance Sample Tables

In addition to TOML metrics, the scraper writes high-cardinality diagnostic data
to dedicated PostgreSQL tables:

```text
oracle_sql_samples
oracle_session_samples
oracle_blocking_session_samples
oracle_database_activity_samples
```

These tables support the Grafana dashboards for:

- Database Activity History (DAH)
- Oracle SQL Performance
- Current Sessions and Blocking

Fields such as `SQL_ID`, child cursor, plan hash, SID, serial number, wait
event, module, program, and machine are intentionally stored in PostgreSQL
instead of being modeled as Prometheus labels.

## Custom Metrics

Custom TOML metrics use the same generic storage path as default TOML metrics:

```text
oracle_metric_samples
```

See [Custom Metrics](../configuration/custom-metrics.md) for the TOML schema.

If a metric has very high cardinality or changes label values constantly, prefer
a PostgreSQL table/dashboard design instead of treating it as a generic metric.
