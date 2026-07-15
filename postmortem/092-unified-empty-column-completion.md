# 092 - Unified empty-column SQL completion

`clutch-complete-select-list-at-point` solved only one symptom: completing columns in `select | from table`.  The same column candidates are useful in other SQL expression positions such as `where |`, `group by |`, `order by |`, and `join ... on |`.

Keeping a SELECT-list-only command would have pushed each new context into its own special case.  That conflicts with the completion model: table names, columns, and keywords should be resolved by the normal completion-at-point path.

The command behind `C-c TAB` now marks the invocation as manual, reuses the same CAPF candidate resolver, and starts `completion-in-region` directly.  That keeps the zero-prefix completion region valid for completion UIs such as Corfu after the command returns.  Manual invocation can offer zero-prefix visible columns at empty SQL expression slots; automatic completion still requires a normal prefix or an explicit `alias.` / `table.` qualifier.

This keeps `TAB` focused on indentation except after an explicit qualifier dot, avoids stealing standard editing keys, and lets empty-position column completion reuse the same statement-scoped table and column metadata path as normal SQL identifier completion.
