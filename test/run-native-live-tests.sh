#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
emacs_bin="${EMACS:-emacs}"
os_name="$(uname -s)"

pg_name="${CLUTCH_TEST_PG_CONTAINER:-clutch-pg-live}"
pg_port="${CLUTCH_TEST_PG_PORT:-55432}"
mysql_name="${CLUTCH_TEST_MYSQL_CONTAINER:-clutch-mysql-live}"
mysql_port="${CLUTCH_TEST_MYSQL_PORT:-55306}"
mongo_name="${CLUTCH_TEST_MONGO_CONTAINER:-clutch-mongo-live}"
mongo_port="${CLUTCH_TEST_MONGO_PORT:-57017}"
pg_image="${CLUTCH_TEST_PG_IMAGE:-docker.io/library/postgres:16}"
mysql_image="${CLUTCH_TEST_MYSQL_IMAGE:-docker.io/library/mysql:8.0}"
mongo_image="${CLUTCH_TEST_MONGO_IMAGE:-docker.io/library/mongo:7}"

started=()
temp_paths=()
container_runtime=""

log() {
  printf '==> %s\n' "$*"
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

runtime_available_p() {
  command -v "$1" >/dev/null 2>&1 && "$1" info >/dev/null 2>&1
}

select_container_runtime() {
  if [[ -n "${CLUTCH_TEST_CONTAINER_RUNTIME:-}" ]]; then
    command -v "$CLUTCH_TEST_CONTAINER_RUNTIME" >/dev/null 2>&1 \
      || die "Container runtime '$CLUTCH_TEST_CONTAINER_RUNTIME' not found"
    "$CLUTCH_TEST_CONTAINER_RUNTIME" info >/dev/null 2>&1 \
      || die "Container runtime '$CLUTCH_TEST_CONTAINER_RUNTIME' is not available"
    container_runtime="$CLUTCH_TEST_CONTAINER_RUNTIME"
  elif [[ "$os_name" == "Linux" ]] && runtime_available_p podman; then
    container_runtime="podman"
  elif runtime_available_p docker; then
    container_runtime="docker"
  else
    die "No available container runtime found. On Linux install/start podman; on macOS start OrbStack."
  fi
}

ctr() {
  "$container_runtime" "$@"
}

require_orbstack_docker() {
  [[ "$container_runtime" == "docker" ]] || return
  [[ "$os_name" == "Darwin" ]] || return
  [[ "${CLUTCH_TEST_ALLOW_NON_ORBSTACK:-}" == "1" ]] && return

  local docker_os
  docker_os="$(docker info --format '{{.OperatingSystem}}' 2>/dev/null || true)"
  [[ "$docker_os" == *OrbStack* ]] \
    || die "macOS live tests require OrbStack-backed Docker. Set CLUTCH_TEST_ALLOW_NON_ORBSTACK=1 to override."

  if command -v orb >/dev/null 2>&1; then
    orb status 2>/dev/null | grep -q "Running" \
      || die "OrbStack is not running"
  fi
}

cleanup() {
  if ((${#started[@]})); then
    for name in "${started[@]}"; do
      log "Removing test container $name"
      ctr rm -f "$name" >/dev/null 2>&1 || true
    done
  fi
  if ((${#temp_paths[@]})); then
    for path in "${temp_paths[@]}"; do
      rm -rf "$path" >/dev/null 2>&1 || true
    done
  fi
}
trap cleanup EXIT

show_container_environment() {
  log "Container runtime: $container_runtime"
  if [[ "$container_runtime" == "docker" ]]; then
    log "Docker context: $(docker context show)"
    log "Docker server: $(docker version --format '{{.Server.Version}}')"
    log "Docker OS: $(docker info --format '{{.OperatingSystem}}')"
  elif [[ "$container_runtime" == "podman" ]]; then
    log "Podman version: $(podman --version)"
  fi
  if [[ "$os_name" == "Darwin" ]] && command -v orb >/dev/null 2>&1; then
    log "OrbStack status:"
    orb status || true
  fi
}

container_running_p() {
  ctr ps --format '{{.Names}}' | grep -Fxq "$1"
}

container_summary() {
  ctr ps --format '{{.ID}} {{.Names}} {{.Ports}}' | grep -F " $1 " || true
}

run_container() {
  if [[ "$container_runtime" == "podman" ]]; then
    ctr run --replace --rm -d "$@"
  else
    ctr run --rm -d "$@"
  fi
}

start_pg() {
  if container_running_p "$pg_name"; then
    log "Reusing PostgreSQL container $pg_name"
    return
  fi
  log "Starting PostgreSQL container $pg_name on 127.0.0.1:$pg_port"
  run_container \
    --name "$pg_name" \
    -e POSTGRES_INITDB_ARGS=--auth-host=md5 \
    -e POSTGRES_PASSWORD=test \
    -p "127.0.0.1:${pg_port}:5432" \
    "$pg_image" \
    -c password_encryption=md5
  started+=("$pg_name")
}

start_mysql() {
  if container_running_p "$mysql_name"; then
    log "Reusing MySQL container $mysql_name"
    return
  fi
  log "Starting MySQL container $mysql_name on 127.0.0.1:$mysql_port"
  run_container \
    --name "$mysql_name" \
    -e MYSQL_ROOT_PASSWORD=test \
    -p "127.0.0.1:${mysql_port}:3306" \
    "$mysql_image"
  started+=("$mysql_name")
}

start_mongo() {
  if container_running_p "$mongo_name"; then
    log "Reusing MongoDB container $mongo_name"
    return
  fi
  log "Starting MongoDB container $mongo_name on 127.0.0.1:$mongo_port"
  run_container \
    --name "$mongo_name" \
    -p "127.0.0.1:${mongo_port}:27017" \
    "$mongo_image"
  started+=("$mongo_name")
}

wait_pg() {
  log "Waiting for PostgreSQL readiness"
  for _ in {1..120}; do
    if ctr exec "$pg_name" pg_isready -U postgres >/dev/null 2>&1; then
      log "PostgreSQL ready: $(container_summary "$pg_name")"
      return
    fi
    sleep 0.5
  done
  echo "PostgreSQL container did not become ready" >&2
  return 1
}

wait_mysql() {
  log "Waiting for MySQL readiness"
  for _ in {1..120}; do
    if ctr exec "$mysql_name" mysqladmin ping -h127.0.0.1 -uroot -ptest >/dev/null 2>&1; then
      log "MySQL ready: $(container_summary "$mysql_name")"
      return
    fi
    sleep 0.5
  done
  echo "MySQL container did not become ready" >&2
  return 1
}

wait_mongo() {
  log "Waiting for MongoDB readiness"
  for _ in {1..120}; do
    if ctr exec "$mongo_name" mongosh --quiet --eval "db.adminCommand({ ping: 1 }).ok" >/dev/null 2>&1; then
      log "MongoDB ready: $(container_summary "$mongo_name")"
      return
    fi
    sleep 0.5
  done
  echo "MongoDB container did not become ready" >&2
  return 1
}

emacs_load_args=(
  --batch -Q
  -L "$repo/../mongo.el"
  -L "$repo/../mysql.el"
  -L "$repo/../pg-el"
  -L "$repo"
  -L "$repo/test"
  -L "$HOME/.emacs.d/straight/repos/mysql.el"
  -L "$HOME/.emacs.d/straight/repos/pg-el"
  --eval "(setq load-prefer-newer t)"
)

run_clutch_live_pg() {
  log "Running UI live tests against PostgreSQL"
  "$emacs_bin" "${emacs_load_args[@]}" \
    -l ert -l clutch-test \
    --eval "(setq clutch-test-backend 'pg clutch-test-host \"127.0.0.1\" clutch-test-port ${pg_port} clutch-test-user \"postgres\" clutch-test-password \"test\" clutch-test-database \"postgres\")" \
    --eval "(ert-run-tests-batch-and-exit '(tag :clutch-live))"
}

run_clutch_live_mysql() {
  log "Running UI live tests against MySQL"
  "$emacs_bin" "${emacs_load_args[@]}" \
    -l ert -l clutch-test \
    --eval "(setq clutch-test-backend 'mysql clutch-test-host \"127.0.0.1\" clutch-test-port ${mysql_port} clutch-test-user \"root\" clutch-test-password \"test\" clutch-test-database \"mysql\")" \
    --eval "(ert-run-tests-batch-and-exit '(tag :clutch-live))"
}

run_db_live_pg() {
  log "Running backend live tests against PostgreSQL"
  "$emacs_bin" "${emacs_load_args[@]}" \
    -l ert -l clutch-db-test \
    --eval "(setq clutch-db-test-pg-host \"127.0.0.1\" clutch-db-test-pg-port ${pg_port} clutch-db-test-pg-user \"postgres\" clutch-db-test-pg-password \"test\" clutch-db-test-pg-database \"postgres\")" \
    --eval "(ert-run-tests-batch-and-exit '(tag :pg-live))"
}

run_db_live_mysql() {
  log "Running backend live tests against MySQL"
  "$emacs_bin" "${emacs_load_args[@]}" \
    -l ert -l clutch-db-test \
    --eval "(setq clutch-db-test-mysql-host \"127.0.0.1\" clutch-db-test-mysql-port ${mysql_port} clutch-db-test-mysql-user \"root\" clutch-db-test-mysql-password \"test\" clutch-db-test-mysql-database \"mysql\")" \
    --eval "(ert-run-tests-batch-and-exit '(tag :mysql-live))"
}

run_db_live_mongodb() {
  log "Running backend live tests against MongoDB native protocol"
  "$emacs_bin" "${emacs_load_args[@]}" \
    -l ert -l clutch-db-test \
    --eval "(setq clutch-db-test-mongodb-live-enabled t clutch-db-test-mongodb-url \"mongodb://127.0.0.1:${mongo_port}/clutch_test\")" \
    --eval "(ert-run-tests-batch-and-exit '(tag :mongodb-live))"
}

select_container_runtime
require_orbstack_docker
show_container_environment
start_pg
start_mysql
start_mongo
wait_pg
wait_mysql
wait_mongo

run_clutch_live_pg
run_clutch_live_mysql
run_db_live_pg
run_db_live_mysql
run_db_live_mongodb
