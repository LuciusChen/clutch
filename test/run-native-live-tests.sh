#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
emacs_bin="${EMACS:-emacs}"

pg_name="${CLUTCH_TEST_PG_CONTAINER:-clutch-pg-live}"
pg_port="${CLUTCH_TEST_PG_PORT:-5432}"
mysql_name="${CLUTCH_TEST_MYSQL_CONTAINER:-clutch-mysql-live}"
mysql_port="${CLUTCH_TEST_MYSQL_PORT:-3306}"

started=()

cleanup() {
  if ((${#started[@]})); then
    for name in "${started[@]}"; do
      docker rm -f "$name" >/dev/null 2>&1 || true
    done
  fi
}
trap cleanup EXIT

container_running_p() {
  docker ps --format '{{.Names}}' | grep -Fxq "$1"
}

start_pg() {
  if container_running_p "$pg_name"; then
    return
  fi
  docker run --rm -d \
    --name "$pg_name" \
    -e POSTGRES_PASSWORD=test \
    -p "127.0.0.1:${pg_port}:5432" \
    postgres:16 >/dev/null
  started+=("$pg_name")
}

start_mysql() {
  if container_running_p "$mysql_name"; then
    return
  fi
  docker run --rm -d \
    --name "$mysql_name" \
    -e MYSQL_ROOT_PASSWORD=test \
    -p "127.0.0.1:${mysql_port}:3306" \
    mysql:8.0 >/dev/null
  started+=("$mysql_name")
}

wait_pg() {
  for _ in {1..120}; do
    if docker exec "$pg_name" pg_isready -U postgres >/dev/null 2>&1; then
      return
    fi
    sleep 0.5
  done
  echo "PostgreSQL container did not become ready" >&2
  return 1
}

wait_mysql() {
  for _ in {1..120}; do
    if docker exec "$mysql_name" mysqladmin ping -h127.0.0.1 -uroot -ptest >/dev/null 2>&1; then
      return
    fi
    sleep 0.5
  done
  echo "MySQL container did not become ready" >&2
  return 1
}

emacs_load_args=(
  --batch -Q
  -L "$repo"
  -L "$repo/test"
  -L "$HOME/.emacs.d/straight/repos/mysql.el"
  -L "$HOME/.emacs.d/straight/repos/pg-el"
)

run_clutch_live_pg() {
  "$emacs_bin" "${emacs_load_args[@]}" \
    -l ert -l clutch-test \
    --eval "(setq clutch-test-backend 'pg clutch-test-host \"127.0.0.1\" clutch-test-port ${pg_port} clutch-test-user \"postgres\" clutch-test-password \"test\" clutch-test-database \"postgres\")" \
    --eval "(ert-run-tests-batch-and-exit '(tag :clutch-live))"
}

run_clutch_live_mysql() {
  "$emacs_bin" "${emacs_load_args[@]}" \
    -l ert -l clutch-test \
    --eval "(setq clutch-test-backend 'mysql clutch-test-host \"127.0.0.1\" clutch-test-port ${mysql_port} clutch-test-user \"root\" clutch-test-password \"test\" clutch-test-database \"mysql\")" \
    --eval "(ert-run-tests-batch-and-exit '(tag :clutch-live))"
}

run_db_live_pg() {
  "$emacs_bin" "${emacs_load_args[@]}" \
    -l ert -l clutch-db-test \
    --eval "(setq clutch-db-test-pg-host \"127.0.0.1\" clutch-db-test-pg-port ${pg_port} clutch-db-test-pg-user \"postgres\" clutch-db-test-pg-password \"test\" clutch-db-test-pg-database \"postgres\")" \
    --eval "(ert-run-tests-batch-and-exit '(tag :pg-live))"
}

run_db_live_mysql() {
  "$emacs_bin" "${emacs_load_args[@]}" \
    -l ert -l clutch-db-test \
    --eval "(setq clutch-db-test-mysql-host \"127.0.0.1\" clutch-db-test-mysql-port ${mysql_port} clutch-db-test-mysql-user \"root\" clutch-db-test-mysql-password \"test\" clutch-db-test-mysql-database \"mysql\")" \
    --eval "(ert-run-tests-batch-and-exit '(tag :mysql-live))"
}

start_pg
start_mysql
wait_pg
wait_mysql

run_clutch_live_pg
run_clutch_live_mysql
run_db_live_pg
run_db_live_mysql
