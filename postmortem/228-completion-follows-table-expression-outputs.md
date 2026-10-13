# Completion follows table-expression outputs

## Problem and decision

Issue #167 confirmed that direct schema-qualified JOINs completed, while a second comma-separated FROM source and aliases of derived SELECTs did not. The original cross-schema tests covered cache isolation and direct aliases, not these SQL shapes. Reproduce through the public CAPF before changing production code.

Keep the existing statement cache as the owner of source positions and derived output plans. A lexical scan distinguishes FROM-list commas from commas in SELECT lists, arguments, literals and comments. Existing table extraction consumes those positions; derived aliases resolve to selected output names, not guessed base-table aliases. Star projections use the existing metadata path, including asynchronous scoped column loading, while named projections need no database request. Query regions keep nested aliases and set-operation branches apart. A temporary map shares parsed subquery outputs across regions during one analysis; only the existing statement cache retains the resulting plans. There is no new metadata cache, backend protocol, dependency or database query for parsing.

This remains a lightweight completion parser. It does not infer writable row identity or derived-column eldoc, resolve CTE outputs or derived-table column-name lists, or model every dialect-specific SELECT modifier. Expressions need explicit AS aliases for portable output names; direct column aliases may omit AS. Unqualified star sources retain the existing backend completion behavior.

## Evidence

The public CAPF regression failed on main for the comma-separated schema qualifier. Independently disabling comma-source detection or derived-alias resolution makes it fail in the respective reported scenario. Its cases now cover comma-source aliases, derived aliases in SELECT and ON, explicit projections, renamed and computed outputs, qualified and nested stars, UNION names and scopes, and commas outside FROM. The existing five-backend live test starts with cold derived-star completion, checks cached requests and absence of current-schema reads, and executes completed comma-list and derived-table SQL against disposable native PostgreSQL/MySQL and JDBC Oracle/SQL Server/file-backed DuckDB databases.

The follow-up review at 28cb50d reproduced five additional boundaries through public CAPF with in-memory SQLite: a projection comma consumed the following FROM, leading comments hid SELECT, a no-FROM trailing clause entered the projection, EXCEPT/INTERSECT stars mixed right-branch names, and SELECT ALL hid star outputs. The existing table-driven regression first failed before the fixes and now includes these inputs, Oracle MINUS and set-branch alias isolation. Shared set-operator recognition keeps source boundaries, query regions and alias scopes aligned; the existing comment stripper and top-level clause scanner handle the other boundaries. The five-backend live test additionally executes leading-comment and SELECT ALL derived projections.

Review of 7681eae exposed a dialect regression: unconditional MINUS recognition truncated a SQLite projection named minus and hid an alias before a WHERE minus condition. Both failures were reproduced through public CAPF on an in-memory SQLite connection, and the existing SQLite regression failed before the correction. Set-operation matching now derives MINUS support from the connection's registered SQL product; alias keyword checks use the same rule, preserving a SQLite alias named minus too. The existing derived-source cases use an Oracle test connection to retain MINUS coverage, and the five-backend live test executes MINUS on Oracle and UNION elsewhere. This adds no metadata request, cache or persistent state.

## Performance

Compiled Emacs 32 code, warmed libraries and cached metadata: a 60,028-byte SELECT with 10,000 projected columns averaged 2.14 ms for first analysis and 0.44 ms for cached completion on main; the new code averaged 3.85 ms and 0.43 ms respectively. The first implementation took 21 ms for first analysis because every projection comma became a scanner candidate. Collecting comma candidates only after the first FROM/JOIN and reusing the source scan removed that overhead. This still adds first-analysis work; it does not make the old path faster. SQL edits invalidate the statement analysis as before.

After the five boundary fixes, the same compiled comparison against 28cb50d measured 3.77 → 7.82 ms first analysis and 0.43 → 0.88 ms cached CAPF for the 60 KB projection stress case; derived cached completion remained 0.005 ms. Preserving a keyword after a rejected projection comma means the extractor now scans that following token rather than skipping it. This is a measured CPU cost of the correctness fix, not a performance improvement; no database requests were added. The final nested stress probe took 20.35 ms at 40 levels.

The Oracle-only MINUS follow-up, compared with 7681eae using compiled code, measured 8.15 → 8.12 ms first analysis and 0.91 → 0.86 ms cached CAPF on the same 60 KB input; derived cached completion stayed at 0.006 ms. These local samples show no material additional cost from dialect selection.

A separate nested-query stress probe found that reparsing each enclosing region took 213 ms at 40 nested derived tables. Sharing output plans during one analysis reduced this to 22 ms; 10 levels took 2.2 ms. This is not a claim of linear parsing for arbitrary nesting.

Separate disposable live probes using final compiled code measured cached CAPF means of 0.039 ms on PostgreSQL, 0.032 ms on MySQL, 0.056 ms on Oracle, 0.089 ms on SQL Server and 0.047 ms on file-backed DuckDB; maxima ranged from 0.122 to 0.268 ms. First-analysis/dispatch calls averaged 0.254–0.541 ms, with a maximum of 1.143 ms. Each backend probe also passed the completed-SQL live test. These are local command durations, not time until asynchronously requested metadata arrives or a bound on remote driver latency. Performance probes are temporary artifacts, not permanent benchmark tests.

## Verification

After correcting the follow-up live fixture to test a leading-comment star projection (explicit SQL names retain their spelling, whereas Oracle metadata names are uppercase), the full native/JDBC matrix passed all 15 suites: 251 passed, 224 capability skips, no failures. The initial matrix stopped on a new fixture assertion that mistook a trailing ON-column qualifier for a table-name slot; its expected value was corrected before the successful run. Temporary containers and volumes were removed; production connections and the installed Emacs plugin were untouched.

The full non-live gate passed on Emacs 29.4, 30.2 and 32: 946 passed and four skipped per version, with byte compilation, package-lint, checkdoc and architecture checks clean.
