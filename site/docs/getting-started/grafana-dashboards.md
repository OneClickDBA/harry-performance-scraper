---
title: Grafana Dashboards
sidebar_position: 4
---

# Grafana Dashboards

Grafana reads the PostgreSQL storage database directly. Configure a PostgreSQL
datasource that points at the same database used by the scraper.

The Docker Compose test stack provisions the datasource automatically from:

```text
docker-compose/grafana/datasources/datasources.yaml
```

Dashboard JSON files are stored in:

```text
docker-compose/grafana/dashboards/
```

## Included Dashboards

### Database Activity History (DAH)

File:

```text
docker-compose/grafana/dashboards/database-activity-history.json
```

Purpose:

- Average active sessions by wait class.
- Top SQL by active samples.
- Top sessions by active samples.
- Activity by `SQL_ID`.
- Recent activity samples.
- Top wait events.
- Module/program activity.

Primary table:

```text
oracle_database_activity_samples
```

This dashboard is the ASH/AAS-style historical view. It intentionally uses
high-cardinality fields that should not be Prometheus labels.

### Oracle SQL Performance

File:

```text
docker-compose/grafana/dashboards/oracle-sql-performance.json
```

Purpose:

- Top SQL by elapsed time delta.
- Top SQL by CPU time delta.
- Top SQL by User I/O wait delta.
- SQL efficiency outliers.
- Selected `SQL_ID` time delta.
- Selected `SQL_ID` plan hashes.
- SQL text lookup.

Primary table:

```text
oracle_sql_samples
```

Use the `SQL_ID` textbox variable to drill into a specific statement.

### Current Sessions and Blocking

File:

```text
docker-compose/grafana/dashboards/oracle-postgresql-performance.json
```

Purpose:

- Current active SQL sessions.
- Current waiters by event.
- Long-running active sessions.
- Blocking sessions.
- Session inventory by user, module, and program.

Primary tables:

```text
oracle_session_samples
oracle_blocking_session_samples
```

This dashboard is the current-state triage view. Historical active-session
graphs live in the DAH dashboard instead.

## Importing Manually

If you are not using the Compose provisioning files:

1. Create a Grafana PostgreSQL datasource.
2. Set its UID to `PostgreSQL`, or edit the dashboard JSON files to match your
   datasource UID.
3. Import the JSON dashboards from `docker-compose/grafana/dashboards/`.

## Required Data

The dashboards assume the scraper is writing these tables:

- `oracle_database_activity_samples`
- `oracle_sql_samples`
- `oracle_session_samples`
- `oracle_blocking_session_samples`

If a dashboard is empty, verify:

- `metrics.scrapeInterval` is configured.
- PostgreSQL `output.postgresql.url` is correct.
- the Oracle monitoring user can query the relevant `GV$` views.
- Grafana time range includes recently collected rows.
