---
title: Multiple Databases
sidebar_position: 3
---

# Scraping Multiple Databases

The scraper can collect from many Oracle databases in the same process. Each
database entry has a local name, and every PostgreSQL row includes that name in
`source_database`.

Grafana dashboards normally select one source database at a time. This keeps
high-cardinality views such as DAH, SQL performance, and current sessions usable
when many databases are collected into the same PostgreSQL backend.

If you are connecting to multiple databases with mTLS, see
[mTLS for multiple databases with Oracle Wallet](./oracle-wallet.md#mtls-for-multiple-databases-with-oracle-wallet).

## Example

```yaml
databases:
  db1:
    username: ${DB1_USERNAME}
    password: ${DB1_PASSWORD}
    url: localhost:1521/freepdb1
    queryTimeout: 5
    connMaxLifetime: 30m
    connMaxIdleTime: 10m
    maxOpenConns: 10
    maxIdleConns: 10

  db2:
    username: ${DB2_USERNAME}
    password: ${DB2_PASSWORD}
    url: localhost:1522/freepdb1
    queryTimeout: 5
    connMaxLifetime: 30m
    connMaxIdleTime: 10m
    maxOpenConns: 10
    maxIdleConns: 10

metrics:
  scrapeInterval: 15s
  default: default-metrics.toml

output:
  postgresql:
    url: ${POSTGRES_URL}
    autoMigrate: true
    retention: 720h
```

Use one entry per service or PDB that must be monitored independently. For RAC
databases, point the connection string at the service you want to monitor; the
queries use `GV$` views where instance-level data is needed.

## Per-Database Metric Selection

Default and custom TOML/YAML metrics run against every configured database unless
the metric definition has a `databases` field.

```toml
[[metric]]
context = "db_platform"
labels = [ "platform_name" ]
metricsdesc = { value = "Database platform." }
request = '''
select platform_name, 1 as value
from gv$database
'''
databases = [ "db2", "db3" ]
```

The example above only runs for the database entries named `db2` and `db3`. If
`databases` is omitted, the metric runs for all configured databases. If
`databases = []`, the metric is disabled.

The built-in performance collectors for DAH, SQL performance, current sessions,
and blocking sessions are database-wide collectors. They write to dedicated
PostgreSQL tables and rely on dashboard variables for database selection.

## Duplicate Connections

If more than one database entry uses the same URL and username, the scraper logs
a warning similar to:

```text
msg="duplicated database connections" "database connections"="db1, db2 count=2
```

Duplicate entries are allowed, but they usually create duplicated PostgreSQL
samples unless each entry has an intentional purpose.
