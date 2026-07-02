---
title: Custom Metrics
sidebar_position: 2
---

# Custom Metrics

The scraper can run arbitrary Oracle SQL queries from TOML or YAML metric files.
These metrics are collected on the scraper schedule and written to PostgreSQL in
`oracle_metric_samples`.

Custom metrics are useful for low-cardinality operational values that do not
need a dedicated PostgreSQL table or dashboard model. For high-cardinality
performance data, such as SQL IDs, sessions, waits, blocking chains, or object
hotspots, prefer a dedicated collector/table/dashboard instead of storing the
data as generic metric samples.

Configure metric files in the main scraper config:

```yaml
metrics:
  scrapeInterval: 15s
  connectionBackoff: 5m
  default: default-metrics.toml
  custom:
    - custom-metrics-example/custom-metrics.toml
```

## Hot Reload

The scraper watches custom metric files. When a file changes, the definition is
reloaded and used by the next scheduled collection cycle.

## Metric Schema

Metrics files contain a series of `[[metric]]` definitions in TOML, or the
equivalent list in YAML.

| Field Name | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `context` | Metric context. Stored in `oracle_metric_samples.context` and used as part of the metric name. | String | Yes | |
| `labels` | Query columns to store as metric labels. Columns not listed as labels are parsed as metric values. | Array of strings | No | |
| `metricsdesc` | Mapping between value columns and human-readable metric descriptions. | Dictionary of strings | Yes | |
| `metricstype` | Mapping between value columns and metric type metadata, usually `gauge` or `counter`. | Dictionary of strings | No | `gauge` |
| `metricsbuckets` | Legacy histogram bucket metadata for compatible metric definitions. | Dictionary | No | |
| `fieldtoappend` | Query column appended to the metric name. This column is not stored as a label. | String | No | |
| `request` | Oracle SQL query to run. Do not include a trailing semicolon. | String | Yes | |
| `ignorezeroresult` | Do not log an error when the query returns no rows. | Boolean | No | `false` |
| `querytimeout` | Query timeout for this metric, for example `300ms`, `10s`, or `0.5h`. | Duration | No | Database `queryTimeout` |
| `scrapeinterval` | Per-metric collection interval. If the metric was already collected inside the interval, cached values are reused. | Duration | No | Global scrape interval |
| `databases` | Database names from the scraper config where this metric should run. Empty or omitted means all databases. | Array of strings | No | All databases |

## Example Metric

This metric stores two values from a simple query:

```toml
[[metric]]
context = "example"
request = "SELECT 1 as value_1, 2 as value_2 FROM DUAL"
metricsdesc = {
  value_1 = "Simple example returning 1.",
  value_2 = "Simple example returning 2."
}
```

The collector writes one row per value column to `oracle_metric_samples`. The
stored data includes the collection timestamp, source database, context, metric
name, metric help, metric type, numeric value, and labels.

```sql
select
  collected_at,
  source_database,
  context,
  metric_name,
  metric_type,
  value,
  labels
from oracle_metric_samples
where context = 'example'
order by collected_at desc;
```

## Labels

Use `labels` for low-cardinality dimensions that help group the metric in SQL or
Grafana:

```toml
[[metric]]
context = "sessions"
labels = [ "inst_id", "status", "type" ]
request = '''
select inst_id, status, type, count(*) as value
from gv$session
group by inst_id, status, type
'''
metricsdesc = { value = "Session count by instance, status, and type." }
metricstype = { value = "gauge" }
```

Rows from this metric store `inst_id`, `status`, and `type` in the `labels`
column. Keep label values bounded; do not use SQL text, SQL ID, SID, serial
number, object names, or similar high-cardinality fields unless the retention and
dashboard impact is intentional.

## Database Selection

By default, each metric runs against every configured Oracle database. Use the
`databases` field to limit a metric to specific database entries:

```toml
[[metric]]
context = "db_platform"
labels = [ "platform_name" ]
request = "select platform_name, 1 as value from gv$database"
metricsdesc = { value = "Database platform." }
databases = [ "prod1", "prod2" ]
```

Set `databases = []` to disable a metric without deleting it from the file.

## Override Existing Metrics

You may override an existing default metric by providing a custom metric with
the same `context` and `metricsdesc` keys. The last file listed in
`metrics.custom` wins when the same metric appears more than once.

```toml
[[metric]]
context = "my_default_metric"
metricsdesc = { value = "Existing metric with a different query timeout." }
request = "select 1 as value from dual"
scrapeinterval = "30s"
querytimeout = "10s"
```

## YAML Metrics

YAML metrics use the same field names as TOML metrics:

```yaml
metrics:
  - context: sessions
    labels: [inst_id, status, type]
    metricsdesc:
      value: Session count by instance, status, and type.
    request: |
      select inst_id, status, type, count(*) as value
      from gv$session
      group by inst_id, status, type
```

The default metric file in this repository is `default-metrics.toml`.

## Container Images

When building a container image, copy the metric file and a config file that
references it:

```Dockerfile
FROM oracledb-performance-scraper:local
COPY custom-metrics.toml /
COPY scraper-config.yaml /
ENTRYPOINT ["/oracledb_performance_scraper", "--config.file", "/scraper-config.yaml"]
```
