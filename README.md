# Harry - Performance Scraper for Oracle Database

<p align="center">
  <img width="256" src="https://oneclickdba.github.io/harry-performance-scraper-web/img/harry/harry.png" alt="Harry - Performance Scraper for Oracle Database">
</p>

Harry is a PostgreSQL-backed Oracle Database performance and operational
monitoring scraper. It collects SQL, session, blocking, database activity,
capacity, resource, and collector-health data, then stores it in PostgreSQL for
Grafana dashboards, alerting, and historical analysis.

Project homepage: [oneclickdba.com/harry](https://oneclickdba.com/harry/).

The project is developed at
[OneClickDBA/harry-performance-scraper](https://github.com/OneClickDBA/harry-performance-scraper)
and originated as a heavily modified fork of Oracle's application observability
codebase. Harry is not a Prometheus exporter: it writes structured samples
directly to PostgreSQL and exposes only small health and readiness endpoints.

> **Oracle Diagnostics Pack**
>
> The Oracle ASH collector is **disabled by default**. Enabling it requires that
> **you verify your Oracle Diagnostics Pack licensing**. The default `session`
> activity source samples `GV$SESSION` and does not query Oracle ASH.

## Features

- Native SQL, session, blocking, database activity, and operational collection.
- Frequent `GV$SQLSTATS` sampling for both long-running and high-frequency SQL.
- Bounded SQL text and execution-plan collection from `GV$SQL` and
  `GV$SQL_PLAN`.
- PostgreSQL range-partitioned storage with automatic retention.
- PostgreSQL-backed active/standby operation using advisory-lock leader election.
- Grafana dashboards and PostgreSQL-backed operational alerts.
- Optional user-defined additional metrics from TOML or YAML definitions.
- Docker Compose testing and production-style Linux service deployment.

## Getting Started

The complete documentation is published at the
[Harry documentation site](https://oneclickdba.github.io/harry-performance-scraper-web/).
Start with:

- [Installation and basic configuration](https://oneclickdba.github.io/harry-performance-scraper-web/docs/getting-started/basics)
- [Configuration reference](https://oneclickdba.github.io/harry-performance-scraper-web/docs/configuration/config-file)
- [Collection and storage model](https://oneclickdba.github.io/harry-performance-scraper-web/docs/getting-started/collection-model)
- [Grafana dashboards](https://oneclickdba.github.io/harry-performance-scraper-web/docs/getting-started/grafana-dashboards)
- [Builds and releases](https://oneclickdba.github.io/harry-performance-scraper-web/docs/releases/builds)

The Docusaurus source is maintained separately in
[OneClickDBA/harry-performance-scraper-web](https://github.com/OneClickDBA/harry-performance-scraper-web).
PostgreSQL schema and Oracle-to-PostgreSQL data-flow diagrams remain in
[`doc/`](doc/) because they are maintained alongside the implementation.

## Build

The default build uses the `godror` driver and requires Oracle Instant Client
at runtime:

```bash
go build -o harry-scraper ./
```

For a no-CGO build using the `go-ora` driver:

```bash
go build -tags goora -o harry-scraper ./
```

## Production Considerations

Use a dedicated Oracle monitoring user with only the grants required for the
enabled collectors. Keep production credentials outside the YAML file, using
environment files, an Oracle wallet, or a supported vault integration.

High availability is enabled by default. Harry instances using the same
PostgreSQL cluster and HA scope elect one active scraper; standbys do not open
Oracle connections or write samples. PostgreSQL HA must independently enforce
a single writable primary with quorum and fencing. See
[High availability](https://oneclickdba.github.io/harry-performance-scraper-web/docs/configuration/high-availability)
for connection-string, scope, readiness, and Patroni requirements.

PostgreSQL-backed Grafana alerts cannot report a complete failure of Grafana,
PostgreSQL, the scraper, or the notification path. Production deployments need
an independent external availability check for the monitoring stack. See
[Grafana Alerting](https://oneclickdba.github.io/harry-performance-scraper-web/docs/configuration/grafana-alerting)
for provisioning and operational requirements.

## Project Information

Harry is developed by Jorge Holgado and commercially supported
under the OneClickDBA brand.

Contact: <dodger@oneclickdba.com>.

- Report security vulnerabilities according to [SECURITY.md](SECURITY.md).
- Project license: [LICENSE.txt](LICENSE.txt).
- Third-party notices: [THIRD_PARTY_LICENSES.txt](THIRD_PARTY_LICENSES.txt).
- Upstream license texts: [`LICENSES/`](LICENSES/).
- Name and visual identity terms: [TRADEMARKS.md](TRADEMARKS.md).

Harry includes software derived from
[`iamseth/oracledb_exporter`](https://github.com/iamseth/oracledb_exporter),
distributed under the MIT License, and
[`oracle/oracle-db-appdev-monitoring`](https://github.com/oracle/oracle-db-appdev-monitoring),
distributed under the Universal Permissive License, Version 1.0.

Original modifications and components developed specifically for Harry are
Copyright (c) 2026 Jorge Holgado and are distributed under the terms in
[LICENSE.txt](LICENSE.txt). The software licenses do not grant permission to use
the Harry name, logo, or visual identity.

Oracle and Oracle Database are trademarks or registered trademarks of Oracle
and/or its affiliates. Harry is an independent project and is not affiliated
with, endorsed by, or sponsored by Oracle.

