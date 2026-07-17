<!-- vim-markdown-toc GFM -->

* [Oracle DB Performance Scraper](#oracle-db-performance-scraper)
    * [Current Focus](#current-focus)
    * [Storage Model](#storage-model)
    * [Build](#build)
    * [Minimal Configuration Shape](#minimal-configuration-shape)
    * [Grafana](#grafana)
    * [Local Testing](#local-testing)
    * [Documentation](#documentation)
    * [Developer](#developer)
    * [Security](#security)
    * [License](#license)

<!-- vim-markdown-toc -->

# Oracle DB Performance Scraper

This project lives at [dodger-one/oracledb-performance-scraper](https://github.com/dodger-one/oracledb-performance-scraper)
and started as a heavily modified fork of Oracle's application observability
codebase.

Oracle DB Performance Scraper is a PostgreSQL-backed Oracle monitoring
scraper built from the Oracle application observability codebase. It
collects Oracle database metrics, SQL/session performance samples, blocking
session data, and Database Activity History style samples, then stores them in
PostgreSQL for Grafana dashboards and longer-term analysis.

This project is not a Prometheus metrics endpoint. The scraper runs on a schedule,
writes samples to PostgreSQL, and exposes only a small health endpoint.

## Current Focus

- Collect native SQL, session, blocking, and database activity samples by
  default.
- Optionally collect additional SQL-derived metrics from TOML or YAML
  definition files such as `oracle-operational-metrics.toml`.
- Collect direct performance samples from Oracle dynamic performance views such
  as `GV$SQL`, `GV$SESSION`, and `GV$ACTIVE_SESSION_HISTORY`.
- Store samples in PostgreSQL range-partitioned tables.
- Use PostgreSQL retention by dropping old daily partitions.
- Visualize data through Grafana dashboards backed directly by PostgreSQL.
- Run either with Docker Compose for testing or as a normal Linux service on
  real servers.

## Storage Model

The scraper writes to PostgreSQL tables such as:

- `oracle_metric_samples`
- `oracle_sql_samples`
- `oracle_sql_texts`
- `oracle_sql_plans`
- `oracle_session_samples`
- `oracle_blocking_session_samples`
- `oracle_database_activity_samples`

Tables are created automatically when `output.postgresql.autoMigrate: true` is
set. Daily partitions are created on demand before samples are written. Complete
SQL text is normalized into the non-partitioned `oracle_sql_texts` lookup table
instead of being duplicated in every SQL sample.
Execution-plan operations from `GV$SQL_PLAN` are deduplicated in the
non-partitioned `oracle_sql_plans` lookup table and retained while their cursor
plan remains referenced by SQL samples.

## Build

The default build uses the `godror` driver and requires Oracle Instant Client at
runtime:

```bash
go build -o oracledb_performance_scraper ./
```

For a no-CGO build without Oracle Instant Client:

```bash
go build -tags goora -o oracledb_performance_scraper ./
```

## Minimal Configuration Shape

```yaml
databases:
  prod:
    username: ${ORACLE_USERNAME}
    password: ${ORACLE_PASSWORD}
    url: ${ORACLE_CONNECT_STRING}
    queryTimeout: 10
    connMaxLifetime: 30m
    connMaxIdleTime: 5m
    maxOpenConns: 10
    maxIdleConns: 10

metrics:
  scrapeInterval: 15s
  definitions:
    - /etc/oracledb-monitor/oracle-operational-metrics.toml

performance:
  sqlPlans:
    enabled: true
    interval: 2m
    topN: 20
    queryTimeout: 10s

output:
  postgresql:
    url: ${POSTGRES_URL}
    autoMigrate: true
    retention: 720h

log:
  level: info
  format: logfmt
  disable: 1

web:
  listenAddresses: [":9161"]
```

`metrics.scrapeInterval` defaults to `15s` when omitted.

## Grafana

Grafana reads PostgreSQL directly. The Docker Compose test stack provisions the
PostgreSQL datasource and imports dashboards from:

- `docker-compose/grafana/dashboards/oracle-sessions-and-blocking.json`
- `docker-compose/grafana/dashboards/database-activity-history.json`
- `docker-compose/grafana/dashboards/oracle-sql-performance.json`

## Local Testing

The `docker-compose/` stack is intended for local testing only. It includes
Oracle test databases, PostgreSQL, Grafana, and the scraper service.

For production-like manual deployment without containers, see:

- `BUILD_ON_REAL_SERVERS.md`

## Documentation

The detailed documentation is published at the
[Oracle DB Performance Scraper documentation site](https://dodger-one.github.io/oracledb-performance-scraper-web/).
Its Docusaurus sources are maintained in the separate
[oracledb-performance-scraper-web repository](https://github.com/dodger-one/oracledb-performance-scraper-web).

PostgreSQL schema and Oracle-to-PostgreSQL data-flow diagrams remain in `doc/`
because they are maintained alongside the implementation.

## Developer

Oracle DB Performance Scraper is developed and maintained by Jorge Holgado
<dodger@oneclickdba.com>.

## Security

Use a dedicated Oracle monitoring user with only the grants required for the
views you need to scrape. Store production credentials outside the YAML file,
for example in environment files, a wallet, or a supported vault integration.

Please consult the [security guide](./SECURITY.md) for responsible security
vulnerability disclosure.

## License

Portions Copyright (c) 2016 Seth Miller.
Portions Copyright (c) 2021, 2026, Oracle and/or its affiliates.
Fork-specific modifications Copyright (c) 2026 Jorge Holgado
<dodger@oneclickdba.com>.

Released under the MIT License and the Universal Permissive License v1.0 as
shown at <https://oss.oracle.com/licenses/upl/>. See `LICENSE.txt`.
