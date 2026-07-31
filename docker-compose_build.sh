#!/bin/bash
set -euo pipefail

export GOOS="${GOOS:-linux}"
export GOARCH="${GOARCH:-amd64}"
export TAGS="${TAGS:-godror}"
export CGO_ENABLED="${CGO_ENABLED:-1}"
export VERSION="${VERSION:-0.0.0-dev}"
export GO_VERSION="${GO_VERSION:-1.26.3}"
export BUILD_DEPS_IMAGE="${BUILD_DEPS_IMAGE:-harry-performance-scraper-build-deps:local}"
export CONTAINER_ENGINE="${CONTAINER_ENGINE:-docker}"

DOCKER_COMPOSE=( 
    "${CONTAINER_ENGINE}"
    'compose'
    '--env-file'
    '.env'
    '-f'
    'docker-compose/compose.yaml'
)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}" || exit 1
echo "## config"
"${DOCKER_COMPOSE[@]}" config --quiet

echo "## build dependency image"
"${CONTAINER_ENGINE}" build \
    -f Dockerfile \
    --target build-deps \
    --build-arg GOOS="${GOOS}" \
    --build-arg GOARCH="${GOARCH}" \
    --build-arg TAGS="${TAGS}" \
    --build-arg CGO_ENABLED="${CGO_ENABLED}" \
    --build-arg GO_VERSION="${GO_VERSION}" \
    -t "${BUILD_DEPS_IMAGE}" \
    .

echo "## build"
"${DOCKER_COMPOSE[@]}" build harry-scraper
echo "## UP"
"${DOCKER_COMPOSE[@]}" up -d postgres free26ai second26ai harry-scraper grafana
echo "## ps"
"${DOCKER_COMPOSE[@]}" ps
echo "## healthz"
curl -fsS http://localhost:9161/healthz
echo "## logs"
"${DOCKER_COMPOSE[@]}" logs -f harry-scraper
echo "## Postgresql table info"
"${DOCKER_COMPOSE[@]}" exec postgres psql -U "${POSTGRES_USER:-harry_monitoring}" -d "${POSTGRES_DB:-harry_monitoring}" -c "select count(*) from oracle_metric_samples;"
"${DOCKER_COMPOSE[@]}" exec postgres psql -U "${POSTGRES_USER:-harry_monitoring}" -d "${POSTGRES_DB:-harry_monitoring}" -c "select count(*) from oracle_sql_samples;"
"${DOCKER_COMPOSE[@]}" exec postgres psql -U "${POSTGRES_USER:-harry_monitoring}" -d "${POSTGRES_DB:-harry_monitoring}" -c "select count(*) from oracle_session_samples;"
"${DOCKER_COMPOSE[@]}" exec postgres psql -U "${POSTGRES_USER:-harry_monitoring}" -d "${POSTGRES_DB:-harry_monitoring}" -c "select count(*) from oracle_database_activity_samples;"
echo "## Shutting down, press enter or cancel"
read -r
"${DOCKER_COMPOSE[@]}" down
