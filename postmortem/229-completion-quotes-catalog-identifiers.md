# Completion quotes catalog identifiers

Issue #170 reports PostgreSQL completion inserting mixed-case catalog names without quotes, so the server folds them to a different name. Applying the completion case-style option to identifiers can likewise change which object SQL names.

Keep identifier candidates in catalog case and mark them with their insertion identity. Match their names without surrounding quotes, then call the existing `clutch-db-escape-identifier` on acceptance. This delegates to native MySQL backticks, PostgreSQL/SQLite double quotes and the existing JDBC dialect implementations instead of guessing backend folding or reserved-word rules in the completion engine. The backend may keep safe names bare, as ClickHouse does. Case style continues to apply to keywords only.

For generated qualified column candidates, preserve the qualifier as written in the query and escape the catalog column separately. For completion inside an existing quoted identifier, match the delimiter-escaped contents and replace the original outer quotes on acceptance, avoiding duplicate quotes and allowing unfinished quotes at point.

Regression coverage reproduces the original bare mixed-case insertion, checks quoted/unfinished identifiers, qualifiers, embedded quotes and keyword separation, executes completed SQL against an in-memory SQLite database, and checks JDBC dialect insertion without claiming live JDBC coverage.
