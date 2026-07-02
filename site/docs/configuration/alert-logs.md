---
title: Alert Logs
sidebar_position: 5
---

# Alert Logs

Collect Oracle alert logs with a log ingestion tool.

The scraper writes Oracle alert log records as JSON lines suitable for collection by tools such as Promtail or Fluent Bit.

Each exported log record includes the source database name:

```json
{
  "timestamp": "2026-03-13T18:37:46.302Z",
  "database": "db2",
  "moduleId": "",
  "ecid": "",
  "message": "Example alert log message"
}
```

Alert logging is configured with the following parameters in the scraper config file:

| Parameter            | Description                                                                 | Default          |
|----------------------|-----------------------------------------------------------------------------|------------------|
| log.destination      | Base alert log file path                                                    | `/log/alert.log` |
| log.interval         | Interval to log records                                                     | `15s`            |
| log.disable          | Disable logging if set to `1`                                               | `0`              |
| log.perDatabaseFiles | When `true`, write one file per database using the pattern `alert-<db>.log` | `false`          |

Example alert log YAML configuration:

```yaml
log:
  # Path of log file
  destination: /opt/scraper/alert.log
  # Interval of log updates
  interval: 15s
  # Optional: write /opt/scraper/alert-<db>.log instead of a shared file
  perDatabaseFiles: true
  ## Set disable to 1 to disable logging
  # disable: 0
```

### Multiple Databases and Alert Log Export

If the scraper is configured to collect from multiple databases, it is recommended to set the `log.perDatabaseFiles` property to `true`, creating a separate log file for each database.

When `perDatabaseFiles` is enabled and `destination` is `/opt/scraper/alert.log`, a database named `db2` writes to `/opt/scraper/alert-db2.log`.
