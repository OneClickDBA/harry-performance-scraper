---
title: Kubernetes
sidebar_position: 3
---

# Kubernetes

The scraper can run in Kubernetes, but it is no longer scraped by Prometheus.
It needs outbound network access to Oracle and PostgreSQL, and it exposes only a
health endpoint on port `9161`.

The examples below assume a namespace named `scraper`.

## Secrets

Create Oracle and PostgreSQL credentials:

```bash
kubectl create secret generic oracle-monitoring-secret \
  --from-literal=username=scraperuser \
  --from-literal=password='CHANGE_ME' \
  -n scraper

kubectl create secret generic postgres-monitoring-secret \
  --from-literal=url='postgres://oracledb_monitoring:CHANGE_ME@postgres:5432/oracledb_monitoring?sslmode=disable' \
  -n scraper
```

## Config Map

Create a scraper configuration file such as `oracle-db-scraper-config.yaml`:

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
  disable: 1

web:
  listenAddresses: [':9161']
```

Create the config map:

```bash
kubectl create cm oracle-db-scraper-config \
  --from-file=oracle-db-scraper-config.yaml \
  -n scraper
```

If you use custom TOML metrics, create another config map for those files and
mount them into the pod. Reference the mounted paths from `metrics.custom`.

## Wallets

If using an Oracle wallet, create a config map or secret from the wallet
directory and mount it into the pod. Set either `TNS_ADMIN` or the per-database
`tnsAdmin` property to the mounted path.

## Deployment Shape

A deployment should provide:

- `CONFIG_FILE=/etc/oracledb-monitor/config.yaml`
- `ORACLE_USERNAME` and `ORACLE_PASSWORD` from the Oracle secret
- `ORACLE_CONNECT_STRING` as an environment variable or config value
- `POSTGRES_URL` from the PostgreSQL secret
- a volume mount for the config file
- a volume mount for `default-metrics.toml`
- optional volume mounts for custom metrics or wallets

The container command should run:

```bash
/oracledb_performance_scraper --config.file=/etc/oracledb-monitor/config.yaml
```

Use a non-root user, a read-only root filesystem, and writable temporary/log
volumes as appropriate for your cluster.

## Service

Expose port `9161` only for health checks or operational access:

```yaml
ports:
  - name: health
    port: 9161
    targetPort: 9161
```

Health check:

```bash
kubectl port-forward svc/oracle-db-scraper 9161:9161 -n scraper
curl http://127.0.0.1:9161/healthz
```

## Prometheus ServiceMonitor

Do not configure a Prometheus ServiceMonitor for this scraper. Oracle metrics
are written to PostgreSQL, not exposed on `/metrics`.

## Grafana

Grafana should use a PostgreSQL datasource connected to the same PostgreSQL
database used by the scraper. Import or provision the dashboards from:

```text
docker-compose/grafana/dashboards/
```
