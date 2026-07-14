#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
emacs_bin="${EMACS:-emacs}"

load_paths=()

add_load_path() {
  if [[ -d "$1" ]]; then
    load_paths+=("$1")
  fi
}

add_load_path "$repo/../mongodb.el"
add_load_path "$repo/../redis.el"
add_load_path "$repo/../mysql.el"
add_load_path "$repo/../pg-el"
add_load_path "$repo"
add_load_path "$repo/test"

if [[ -n "${CLUTCH_EXTRA_LOAD_PATH:-}" ]]; then
  IFS=: read -r -a extra_paths <<<"$CLUTCH_EXTRA_LOAD_PATH"
  for path in "${extra_paths[@]}"; do
    add_load_path "$path"
  done
fi

emacs_args=(
  -Q --batch
  --eval "(require 'package)"
  --eval "(package-initialize)"
  --eval "(setq load-prefer-newer t)"
)

for path in "${load_paths[@]}"; do
  emacs_args+=(-L "$path")
done

run_emacs() {
  "$emacs_bin" "${emacs_args[@]}" "$@"
}

usage() {
  cat <<'EOF'
Usage: test/run-ci.sh [TARGET...]

Targets:
  all            Run every non-live CI check.
  main           Run the main ERT suite.
  smoke          Run minimal tagged non-live coverage.
  db             Run the database backend unit suite.
  db-contract    Run backend contract unit tests.
  db-cross       Run cross-backend live tests using configured credentials.
  db-jdbc        Run JDBC backend unit tests.
  db-mongodb     Run MongoDB backend unit tests.
  db-mysql       Run MySQL backend unit tests.
  db-pg          Run PostgreSQL backend unit tests.
  db-redis       Run Redis backend unit tests.
  db-sqlite      Run SQLite backend unit tests.
  db-live        Run database backend live tests using configured credentials.
  byte-compile   Byte-compile distributable clutch*.el files.
  package-lint   Run package-lint with clutch.el as package metadata source.
  checkdoc       Run checkdoc on distributable clutch*.el files.
  architecture   Check Clutch module dependency boundaries.
  native-live    Run native backend/UI live tests against local containers.
EOF
}

run_main_tests() {
  run_main_tests_matching "${CLUTCH_TEST_SELECTOR:-t}"
}

run_main_tests_matching() {
  local selector="$1"
  local -a modules args
  IFS=: read -r -a modules <<<"${CLUTCH_TEST_MODULES:-clutch-test}"
  args=(-l ert -l clutch)
  local module
  for module in "${modules[@]}"; do
    args+=(-l "$module")
  done
  run_emacs \
    "${args[@]}" \
    --eval "(ert-run-tests-batch-and-exit $selector)"
}

run_db_tests() {
  local selector="${CLUTCH_TEST_SELECTOR:-}"
  if [[ -z "$selector" ]]; then
    selector="'(not (tag :db-live))"
  fi
  run_db_tests_matching "$selector"
}

run_db_tests_matching() {
  local selector="$1"
  run_emacs \
    -l ert \
    -l clutch-db-jdbc \
    -l clutch-db-test \
    --eval "(ert-run-tests-batch-and-exit $selector)"
}

run_byte_compile() {
  (
    cd "$repo"
    run_emacs --eval "(setq byte-compile-error-on-warn t)" \
      -f batch-byte-compile clutch*.el
  )
}

run_package_lint() {
  (
    cd "$repo"
    run_emacs \
      --eval "(setq package-archives '((\"melpa\" . \"https://melpa.org/packages/\")))" \
      -l package-lint \
      --eval "(setq package-lint-main-file \"clutch.el\")" \
      -f package-lint-batch-and-exit \
      clutch*.el
  )
}

run_checkdoc() {
  (
    cd "$repo"
    run_emacs \
      --eval "(require 'checkdoc)" \
      --eval "(dolist (file (directory-files default-directory t \"^clutch.*\\.el$\")) (checkdoc-file file))" \
      --eval "(dolist (name '(\"*Warnings*\" \"*warn*\")) (when-let ((buf (get-buffer name))) (with-current-buffer buf (goto-char (point-min)) (when (re-search-forward \"^Warning\" nil t) (princ (buffer-string)) (kill-emacs 1)))))"
  )
}

run_architecture() {
  run_emacs -l check-architecture
}

run_native_live() {
  "$repo/test/run-native-live-tests.sh"
}

run_target() {
  case "$1" in
    all)
      CLUTCH_TEST_MODULES=clutch-test run_main_tests_matching t
      run_db_tests_matching "'(not (tag :db-live))"
      run_byte_compile
      run_package_lint
      run_checkdoc
      ;;
    smoke)
      run_main_tests_matching "'(tag :smoke)"
      run_db_tests_matching "'(and (not (tag :db-live)) (tag :smoke))"
      ;;
    main) run_main_tests ;;
    db) run_db_tests ;;
    db-live) run_db_tests_matching "'(tag :db-live)" ;;
    db-contract)
      run_db_tests_matching \
        "'(and (not (tag :db-live))
               (not (or \"^clutch-db-test-jdbc-\"
                        \"^clutch-db-test-mongodb-\"
                        \"^clutch-db-test-mysql-\"
                        \"^clutch-db-test-native-mysql-\"
                        \"^clutch-db-test-native-pg-\"
                        \"^clutch-db-test-pg-\"
                        \"^clutch-db-test-redis-\"
                        \"^clutch-db-test-sql-interface-mongodb-\"
                        \"^clutch-db-test-sqlite-\"
                        \"^clutch-db-test-cross-\")))"
      ;;
    db-cross) run_db_tests_matching '"^clutch-db-test-cross-"' ;;
    db-jdbc)
      run_db_tests_matching \
        "'(and \"^clutch-db-test-jdbc-\"
               (not (tag :db-live)))"
      ;;
    db-mongodb)
      run_db_tests_matching \
        "'(and (or \"^clutch-db-test-mongodb-\"
                   \"^clutch-db-test-sql-interface-mongodb-\")
               (not (tag :db-live)))"
      ;;
    db-mysql)
      run_db_tests_matching \
        "'(and (or \"^clutch-db-test-mysql-\"
                   \"^clutch-db-test-native-mysql-\")
               (not (tag :db-live)))"
      ;;
    db-pg)
      run_db_tests_matching \
        "'(and (or \"^clutch-db-test-pg-\"
                   \"^clutch-db-test-native-pg-\")
               (not (tag :db-live)))"
      ;;
    db-redis)
      run_db_tests_matching \
        "'(and \"^clutch-db-test-redis-\"
               (not (tag :db-live)))"
      ;;
    db-sqlite)
      run_db_tests_matching \
        "'(and \"^clutch-db-test-sqlite-\"
               (not (tag :db-live)))"
      ;;
    byte-compile) run_byte_compile ;;
    package-lint) run_package_lint ;;
    checkdoc) run_checkdoc ;;
    architecture) run_architecture ;;
    native-live) run_native_live ;;
    -h|--help) usage ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
}

if (($# == 0)); then
  set -- all
fi

for target in "$@"; do
  run_target "$target"
done
