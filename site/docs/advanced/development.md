---
title: Development
sidebar_position: 3
---

# Development

The scraper is a Go program that connects to Oracle databases on a schedule and
writes collected samples into PostgreSQL.

The runtime flow is:

- Parse `--config.file` and expand environment variables.
- Load database, metric, PostgreSQL output, logging, and web configuration.
- Load `default-metrics.toml` and any configured custom metric files.
- Open Oracle connection pools for each configured source database.
- Open the PostgreSQL connection pool and run schema migrations when
  `output.postgresql.autoMigrate` is enabled.
- Start the lightweight web listener for `/healthz` and `/`.
- Start scheduled collectors for default metrics, custom metrics, performance
  samples, session samples, blocking samples, DAH samples, and alert logs.

Generic TOML/YAML metrics are written to `oracle_metric_samples`. Performance
collectors write to dedicated tables such as `oracle_sql_samples`,
`oracle_session_samples`, `oracle_blocking_session_samples`, and
`oracle_database_activity_samples`.

Grafana dashboards read PostgreSQL directly. The scraper does not expose Oracle
metrics on `/metrics`.

## Container Build

To build a container image, run:

```bash
make docker
```

For ARM:

```bash
make docker-arm
```

## Binary Build

The default build uses the `godror` driver:

```bash
go build -o oracledb_performance_scraper ./
```

Build with the pure-Go `go-ora` driver:

```bash
go build -tags goora -o oracledb_performance_scraper ./
```

The Makefile release build writes binaries and archives under `dist`.
