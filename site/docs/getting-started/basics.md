---
title: Installation
sidebar_position: 1
---

# Installation

The scraper needs three services:

- Oracle Database, the source of monitoring data.
- PostgreSQL, the storage backend.
- Grafana, the visualization layer.

The scraper process itself can run as a container, in Docker Compose for local
testing, in Kubernetes, or as a normal Linux service. For production-like
installations, the standalone binary plus systemd is usually the clearest
deployment model.

## Database Permissions

Create the monitoring user in the PDB that you want to monitor. A user created
only in `CDB$ROOT` does not automatically monitor application PDB sessions.

For quick testing, `SELECT_CATALOG_ROLE` is the easiest grant:

```sql
create user scraperuser identified by "CHANGE_ME";
grant create session to scraperuser;
grant select_catalog_role to scraperuser;
```

For more controlled grants, the built-in default metrics and performance
samplers need access to these views:

```sql
grant select on sys.dba_tablespace_usage_metrics to scraperuser;
grant select on sys.dba_tablespaces to scraperuser;
grant select on sys.dba_temp_free_space to scraperuser;
grant select on sys.gv_$instance to scraperuser;
grant select on sys.gv_$system_wait_class to scraperuser;
grant select on sys.gv_$asm_diskgroup_stat to scraperuser;
grant select on sys.gv_$datafile to scraperuser;
grant select on sys.gv_$sysstat to scraperuser;
grant select on sys.gv_$process to scraperuser;
grant select on sys.gv_$session to scraperuser;
grant select on sys.gv_$resource_limit to scraperuser;
grant select on sys.gv_$parameter to scraperuser;
grant select on sys.gv_$database to scraperuser;
grant select on sys.gv_$active_session_history to scraperuser;
grant select on sys.gv_$sql to scraperuser;
grant select on sys.gv_$sqlstats to scraperuser;
grant select on sys.gv_$con_sysmetric to scraperuser;
grant select on sys.v_$diag_alert_ext to scraperuser; -- for alert logs only
```

Additional permissions may be required for custom metrics, depending on the
tables and views used by those TOML files.

Database Activity History uses `GV$ACTIVE_SESSION_HISTORY`. Confirm the
applicable Oracle licensing requirements before relying on ASH-derived
reporting in production.

## PostgreSQL Setup

Create a PostgreSQL database and user for the scraper:

```sql
create database oracledb_monitoring;
create user oracledb_monitoring with encrypted password 'CHANGE_ME';
grant all privileges on database oracledb_monitoring to oracledb_monitoring;

\c oracledb_monitoring

grant all on schema public to oracledb_monitoring;
alter schema public owner to oracledb_monitoring;
```

The scraper can create its own tables and indexes when
`output.postgresql.autoMigrate: true` is set.

## Build The Binary

The default build uses `godror` and requires Oracle Instant Client at runtime:

```bash
go build -o oracledb_performance_scraper ./
```

The no-CGO build uses the `go-ora` driver and does not require Oracle Instant
Client:

```bash
go build -tags goora -o oracledb_performance_scraper ./
```

Install the binary and default metrics file:

```bash
sudo install -m 0755 oracledb_performance_scraper /usr/local/bin/oracledb_performance_scraper
sudo mkdir -p /etc/oracledb-monitor /var/log/oracledb-monitor
sudo cp default-metrics.toml /etc/oracledb-monitor/default-metrics.toml
```

## Minimal Configuration

Create `/etc/oracledb-monitor/config.yaml`:

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
  default: /etc/oracledb-monitor/default-metrics.toml
  custom: []

output:
  postgresql:
    url: ${POSTGRES_URL}
    autoMigrate: true
    retention: 720h

log:
  level: info
  format: logfmt
  destination: /var/log/oracledb-monitor/alert.log
  interval: 15s
  disable: 1

web:
  listenAddresses: [':9161']
```

Keep `metrics.scrapeInterval` configured. The PostgreSQL-backed scraper is
designed for scheduled collection.

Create `/etc/oracledb-monitor/env`:

```bash
ORACLE_USERNAME=scraperuser
ORACLE_PASSWORD=CHANGE_ME
ORACLE_CONNECT_STRING=oracle-host.example.com:1521/APP_PDB
POSTGRES_URL=postgres://oracledb_monitoring:CHANGE_ME@127.0.0.1:5432/oracledb_monitoring?sslmode=disable
```

Lock it down:

```bash
sudo chown root:root /etc/oracledb-monitor/env
sudo chmod 0600 /etc/oracledb-monitor/env
```

## systemd Service

Create `/etc/systemd/system/oracledb-monitor.service`:

```ini
[Unit]
Description=Oracle DB Performance Scraper to PostgreSQL
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=oracledb-monitor
Group=oracledb-monitor
EnvironmentFile=/etc/oracledb-monitor/env
WorkingDirectory=/etc/oracledb-monitor
ExecStart=/usr/local/bin/oracledb_performance_scraper --config.file=/etc/oracledb-monitor/config.yaml
Restart=always
RestartSec=5
TimeoutStopSec=30

NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

Create the service user and start the service:

```bash
sudo useradd --system --home-dir /etc/oracledb-monitor --shell /sbin/nologin oracledb-monitor
sudo chown -R oracledb-monitor:oracledb-monitor /etc/oracledb-monitor /var/log/oracledb-monitor
sudo systemctl daemon-reload
sudo systemctl enable --now oracledb-monitor
```

Useful checks:

```bash
journalctl -u oracledb-monitor -f
curl http://127.0.0.1:9161/healthz
```

## Docker Compose

The `docker-compose/` stack is intended for local testing only. It starts test
Oracle databases, PostgreSQL, Grafana, and the scraper service.

```bash
DB_PASSWORD='<choose-a-local-demo-password>' make docker-compose-up
```

The sample Oracle listeners are intended for localhost development. Do not
expose the Compose stack on a shared or public host.

When the stack is running:

- Scraper health: <http://localhost:9161/healthz>
- Grafana: <http://localhost:3000>
- PostgreSQL: exposed according to the Compose file

Grafana is provisioned with the PostgreSQL datasource and the bundled
dashboards.

```bash
make docker-compose-down
```
