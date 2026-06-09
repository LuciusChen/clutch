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

add_load_path "$repo/../mongo.el"
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
  db             Run the database backend ERT suite.
  byte-compile   Byte-compile distributable clutch*.el files.
  package-lint   Run package-lint with clutch.el as package metadata source.
  checkdoc       Run checkdoc on distributable clutch*.el files.
  native-live    Run MySQL/PostgreSQL/MongoDB live tests against local containers.
EOF
}

run_main_tests() {
  run_emacs \
    -l ert \
    -l clutch \
    -l clutch-test \
    --eval "(ert-run-tests-batch-and-exit)"
}

run_db_tests() {
  run_emacs \
    -l ert \
    -l clutch-db-jdbc \
    -l clutch-db-test \
    --eval "(ert-run-tests-batch-and-exit)"
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

run_native_live() {
  "$repo/test/run-native-live-tests.sh"
}

run_target() {
  case "$1" in
    all)
      run_main_tests
      run_db_tests
      run_byte_compile
      run_package_lint
      run_checkdoc
      ;;
    main) run_main_tests ;;
    db) run_db_tests ;;
    byte-compile) run_byte_compile ;;
    package-lint) run_package_lint ;;
    checkdoc) run_checkdoc ;;
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
