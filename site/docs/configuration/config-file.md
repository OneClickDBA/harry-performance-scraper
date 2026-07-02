---
title: Scraper Configuration
sidebar_position: 1
---

# Scraper Configuration

Configure the scraper with a YAML file passed by `--config.file` or the
`CONFIG_FILE` environment variable.

Environment variables of the form `${VAR_NAME}` are expanded when the file is
loaded. To use a literal `$`, escape it as `$$`.

## Example

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
  connectionBackoff: 5m
  default: /etc/oracledb-monitor/default-metrics.toml
  custom: []

output:
  postgresql:
    url: ${POSTGRES_URL}
    autoMigrate: true
    retention: 720h
    maxConns: 4

log:
  level: info
  format: logfmt
  destination: /var/log/oracledb-monitor/alert.log
  interval: 15s
  disable: 1

web:
  listenAddresses: [':9161']
```

## Database Configuration

Each entry under `databases` defines one Oracle database connection. Monitoring
multiple PDBs normally means adding one named database entry per PDB.

Common properties:

- `username`: Oracle username.
- `password`: Oracle password.
- `passwordFile`: Optional file containing the Oracle password.
- `url`: Oracle connect string, service name, or TNS alias.
- `queryTimeout`: Timeout in seconds for scraper queries. Defaults to `5`.
- `connMaxLifetime`: Maximum age for a pooled Oracle connection. Defaults to
  `30m`.
- `connMaxIdleTime`: Maximum idle time for a pooled Oracle connection. Defaults
  to `5m`.
- `maxOpenConns`: Maximum open Go SQL connections. Defaults to `10`.
- `maxIdleConns`: Maximum idle Go SQL connections. Defaults to `10`.
- `externalAuth`: Use Oracle external authentication.
- `role`: Optional Oracle role such as `SYSDBA`; use the least privileged role
  possible.
- `tnsAdmin`: Wallet or TNS directory for this database connection.
- `labels`: Static labels added to generic metric samples.

The scraper may also load database credentials from [OCI Vault](./oci-vault.md),
[Azure Vault](./azure-vault.md), or [HashiCorp Vault](./hashicorp-vault.md).

## Metrics Configuration

```yaml
metrics:
  scrapeInterval: 15s
  connectionBackoff: 5m
  default: /etc/oracledb-monitor/default-metrics.toml
  custom:
    - /etc/oracledb-monitor/custom-metrics.toml
```

- `scrapeInterval`: How often the scraper collects Oracle data and writes to
  PostgreSQL. Configure this for continuous collection.
- `connectionBackoff`: How long to wait before retrying an invalid database
  connection. Defaults to `5m`.
- `default`: Path to the default TOML metrics file.
- `custom`: Optional list of additional TOML metric files.
- `databaseLabel`: Label key used for generic metric samples. Defaults to
  `database`.

Custom TOML metrics are stored in `oracle_metric_samples`. High-cardinality
Oracle performance samplers write to the dedicated SQL/session/DAH tables.

## PostgreSQL Output

```yaml
output:
  postgresql:
    url: ${POSTGRES_URL}
    autoMigrate: true
    retention: 720h
    maxConns: 4
```

Properties:

- `url`: PostgreSQL connection URL. Required.
- `autoMigrate`: Create tables and indexes on startup. Defaults to `true`.
- `retention`: Optional retention duration. When set, expired daily partitions
  are dropped across all sample tables. Use Go duration syntax such as `720h`
  for 30 days.
- `maxConns`: Maximum PostgreSQL pool connections. Defaults to `4`.
- `minConns`: Minimum PostgreSQL pool connections. Defaults to `0`.
- `connMaxLifetime`: PostgreSQL pool connection lifetime. Defaults to `1h`.

The table name properties are optional. Defaults are:

```yaml
output:
  postgresql:
    samplesTable: oracle_metric_samples
    sqlSamplesTable: oracle_sql_samples
    sessionSamplesTable: oracle_session_samples
    blockingSessionsTable: oracle_blocking_session_samples
    databaseActivityTable: oracle_database_activity_samples
```

Schema-qualified names may be used, for example:

```yaml
output:
  postgresql:
    samplesTable: monitoring.oracle_metric_samples
```

## Logging

The `log` section configures process logs and optional alert-log export.

- `level`: `debug`, `info`, `warn`, or `error`. Defaults to `info`.
- `format`: `logfmt` or `json`. Defaults to `logfmt`.
- `destination`: Alert log output file path. Defaults to `/log/alert.log`.
- `interval`: Alert log polling interval. Defaults to `15s`.
- `disable`: Set to `1` to disable alert-log export. Defaults to `0`.
- `perDatabaseFiles`: Write alert logs to per-database files. Defaults to
  `false`.

## Web Listener

The scraper HTTP listener exposes health only:

- `/healthz`: returns `ok` when the process is running.
- `/`: small landing page pointing to health.

It does not expose Oracle metrics on `/metrics`.

```yaml
web:
  listenAddresses: [':9161']
  readHeaderTimeout: 10s
  readTimeout: 30s
  idleTimeout: 120s
```

Properties:

- `listenAddresses`: One or more addresses to bind. Defaults to `[':9161']`.
- `readHeaderTimeout`: Maximum time to read request headers. Defaults to `10s`.
- `readTimeout`: Maximum time to read a full HTTP request. Defaults to `30s`.
- `idleTimeout`: Maximum keep-alive idle time. Defaults to `120s`.

## Container Image Config

To add a config file to a custom image:

```Dockerfile
FROM oracledb-performance-scraper:local
COPY my-scraper-config.yaml /
ENTRYPOINT ["/oracledb_performance_scraper", "--config.file", "/my-scraper-config.yaml"]
```
