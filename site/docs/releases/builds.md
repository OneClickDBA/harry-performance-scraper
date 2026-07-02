---
title: Builds
sidebar_position: 3
---

# Builds

The scraper can be built with either Oracle's `godror` driver or the no-CGO
`go-ora` driver.

## Default Build

The default build uses `godror`.

```bash
go build -o oracledb_performance_scraper ./
```

Use this build when Oracle Instant Client is installed and configured on the
machine that runs the scraper.

## No-CGO Build

Build with `go-ora` using the `goora` tag:

```bash
go build -tags goora -o oracledb_performance_scraper ./
```

Use this build when you want a simpler binary deployment without Oracle Instant
Client.

## Container Build

The Dockerfile uses Oracle Linux 8 slim as the base image. It has a reusable
`build-deps` stage and runtime stages for `godror` and `goora`.

Build the dependency image:

```bash
docker build \
  -f Dockerfile \
  --target build-deps \
  --build-arg GOOS=linux \
  --build-arg GOARCH=amd64 \
  --build-arg TAGS=godror \
  --build-arg CGO_ENABLED=1 \
  --build-arg GO_VERSION=1.26.3 \
  -t oracledb-performance-scraper-build-deps:local \
  .
```

Build the scraper image through Compose:

```bash
docker compose --env-file .env -f docker-compose/compose.yaml build dbscraper
```

The Compose build creates:

```text
oracledb-performance-scraper:local
```

## Extracting The Binary From An Image

From a running container:

```bash
docker cp <container_name>:/oracledb_performance_scraper ./oracledb_performance_scraper
chmod +x ./oracledb_performance_scraper
```

From an image without keeping a container:

```bash
docker create --name tmp_oracledb_performance_scraper oracledb-performance-scraper:local
docker cp tmp_oracledb_performance_scraper:/oracledb_performance_scraper ./oracledb_performance_scraper
docker rm tmp_oracledb_performance_scraper
chmod +x ./oracledb_performance_scraper
```

The same pattern works with `podman cp`.
