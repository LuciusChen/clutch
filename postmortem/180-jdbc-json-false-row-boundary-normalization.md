# 180 — JDBC JSON false normalized at the row boundary

## Context

The JDBC agent returns booleans as JSON values. Clutch decodes response payloads with `json-parse-string` using `clutch-jdbc--json-false` as the false object, so a SQL boolean false arrives in Elisp as a private sentinel symbol, while true arrives as `t` and SQL NULL as `nil`. That split is necessary inside the JDBC protocol layer, where JSON false and SQL NULL must stay distinct.

Rows passed to the generic result UI without normalization, however: `clutch-jdbc--normalize-row` only unwrapped blob/clob values. The generic formatter did not know the private symbol, so a false cell displayed and copied as `clutch-jdbc-json-false`, and an edit buffer prefilled the same literal text; committing the edit sent that string back to the database instead of boolean false. A partial UI check recognized the symbol by name for JSON serialization, but that was compensating code in the wrong layer and still left display, copy, and edit broken.

## Decision

Normalize at the JDBC row boundary: `clutch-jdbc--normalize-row` now maps every `clutch-jdbc--json-false` occurrence to Clutch's generic `:false`, including values nested in lists and vectors. The UI predicate was reduced to recognizing only `:false`; the private symbol no longer has meaning outside `clutch-db-jdbc.el`. Tests that previously used a fake `:json-false` literal were corrected to use the real sentinel, and new coverage pins top-level, list, and vector normalization.

## Why

The JDBC layer is the only place where the private sentinel exists. Converting it there makes every downstream consumer — display, copy/export, edit prefill, JSON viewing, and write-back — see the same generic representation the rest of Clutch already handles. Keeping the private symbol out of the UI removes fragile string-name matching and closes the whole class of sentinel leaks instead of patching each consumer. `:false` is already Clutch's generic JSON false sentinel elsewhere, so no new rendering contract was needed.

## Verification

The regression test fails on the pre-fix code and passes after: a row containing the sentinel at top level, inside a list, and inside a vector normalizes to `:false` in every position. The full non-live CI suite passes, including byte-compile, package-lint, and checkdoc. A live JDBC agent + H2 round-trip shows false cells displaying as `false`, the edit buffer prefilled with `false`, and the value still `false` after binding the edited text back through the BOOLEAN parameter path.
