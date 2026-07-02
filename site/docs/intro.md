---
sidebar_position: 1
---

# Oracle DB Performance Scraper

Oracle DB Performance Scraper collects Oracle database performance data on a
schedule and stores it in PostgreSQL for Grafana dashboards and troubleshooting.

This fork is no longer a Prometheus metrics endpoint. It does not expose Oracle
metrics on `/metrics`. Instead, it:

- connects to one or more Oracle databases,
- collects default and custom TOML metrics,
- collects SQL, session, blocking-session, and Database Activity History style
  samples,
- writes those samples to PostgreSQL,
- exposes a small health endpoint on `/healthz`,
- lets Grafana read PostgreSQL directly.

The design goal is to keep Prometheus-style, low-cardinality monitoring
separate from high-cardinality diagnostic data. Details such as `SQL_ID`, child
cursor, plan hash, SID, serial number, module, machine, wait event, and blocking
session are useful for investigations but are poor Prometheus labels. They are a
better fit for PostgreSQL tables and SQL-backed Grafana dashboards.

## Main Features

- Scheduled Oracle scraping controlled by `metrics.scrapeInterval`.
- PostgreSQL storage using batched inserts.
- Daily range-partitioned PostgreSQL sample tables created on demand.
- Optional PostgreSQL retention that drops old daily partitions.
- Default Oracle metric collection from `default-metrics.toml`.
- Optional custom TOML metrics.
- Direct performance collection from Oracle dynamic performance views:
  - `GV$SQL`
  - `GV$SESSION`
  - `GV$ACTIVE_SESSION_HISTORY`
- Database Activity History fallback from current session samples when ASH is
  unavailable or empty.
- Grafana dashboards backed by PostgreSQL:
  - Database Activity History (DAH)
  - Oracle SQL Performance
  - Current Sessions and Blocking
- Oracle alert log export to JSON files.
- Oracle wallet, external authentication, OCI Vault, Azure Vault, and HashiCorp
  Vault credential integrations inherited from the upstream codebase.
- Builds with either `godror` and Oracle Instant Client, or the no-CGO `go-ora`
  driver using `-tags goora`.

## PostgreSQL Tables

The scraper writes to these primary PostgreSQL tables:

- `oracle_metric_samples`
- `oracle_sql_samples`
- `oracle_session_samples`
- `oracle_blocking_session_samples`
- `oracle_database_activity_samples`

When `output.postgresql.autoMigrate: true` is configured, the scraper creates
the parent partitioned tables and indexes automatically. Daily child partitions
are created just before data is written.

## Supported Oracle Versions

The scraper is intended for Oracle Database 19c and newer. Some dashboards need
access to `GV$ACTIVE_SESSION_HISTORY`; if that view is unavailable or not
granted, the DAH collector falls back to active session samples where possible.

## Acknowledgements

Oracle DB Performance Scraper is developed and maintained by Jorge Holgado
<dodger@oneclickdba.com>.

This project started from Oracle's application observability codebase, which
itself incorporated earlier Oracle monitoring work from Seth Miller. The current
fork changes the runtime model from Prometheus exposition to scheduled
collection into PostgreSQL.
