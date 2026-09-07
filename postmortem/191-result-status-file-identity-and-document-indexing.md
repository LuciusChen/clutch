# Preserve server state and file identity; index document grids

## Evidence

The 2026-09-06 commit review confirmed two old correctness defects independently of the recent lossless-result fixes. Native MySQL discarded final result EOF status, so SELECT FOR UPDATE in Manual mode left the client reporting no transaction; entering an empty staged batch released a real InnoDB row lock. Separate local Org directories with distinct relative SQLite app.db files shared one Babel cache entry and returned the first database's data.

MongoDB grid conversion still repeatedly searched growing key lists and each document alist. The 1,000-document baseline grew from about 10 ms at 32 fields to 317 ms at 256 fields. This is separate from the already repaired protocol BSON encoding path.

## Decision

Keep MySQL transaction state in mysql.el: text and binary row readers publish the final server status, including zero. Clutch's existing transaction/savepoint policy does not need a compensating query or another transaction layer.

Clutch's public connection preparation owns local SQLite filename resolution. Reuse its existing SQLite filename normalizer with the source directory, preserving :memory: and the empty temporary-database name. Babel prepares SQLite params before forming its cache key and opens those same params. Network backends retain prepare-on-cache-miss so a reused TRAMP connection does not prompt again. Remote SQLite support is not added; remote command-source handling stays unchanged.

Use a result-local field index for MongoDB grids, walk each document in order and fill one row vector. Accumulate column categories while filling rows instead of collecting another list of values for every column. Preserve first-seen column order with _id first, first duplicate-key lookup behavior, missing versus explicit null when inferring types, BSON wrappers and the identical hidden source document. Remove the obsolete separate column sampling pass, not the public list-of-rows contract.

## Verification boundary

Require failing regressions before implementation, real SQLite Babel calls, native MySQL lock-retention verification, grid contract tests and the same width-scaling benchmark. Run non-live suites, native live workflows and compilation/lint checks. JDBC stale-handle ownership and non-cooperative cancel remain separately classified follow-ups, not reasons to enlarge this repair.

## Verification results (2026-09-06)

The final Babel regression fails against the old cache implementation by returning directory A's marker when executing in directory B. The MySQL result-status and MongoDB traversal regressions also fail before their respective fixes. With the fixes, the native MySQL lock test passes for both text and prepared queries: an independent session times out while the lock is held and can update after explicit rollback.

- Clutch main and backend suites: 569 + 235 passed.
- MySQL and Babel suites: 97 passed; 28 opt-in MySQL live cases were not enabled in this unit run.
- Native PostgreSQL, MySQL, MongoDB and Redis workflows: 64 passed, 7 skipped for non-selected backends. This matrix includes the new MySQL lock-retention regression; it does not replace the separate MySQL protocol/TLS live suite.
- MongoDB: 1,000 deterministic heterogeneous, sparse and duplicate-key result comparisons match the previous implementation.
- All 23 distributable source files and the MySQL/Babel test files compile with warnings treated as errors. Checkdoc, Clutch/MySQL package lint and the architecture checks pass. Optional ob-clutch package lint still reports its two pre-existing public-option/group prefix diagnostics; those names are unchanged.
- Rebuilt the three affected straight packages and passed seven targeted regressions while loading their actual installed bytecode.

The matched benchmark byte-compiles each full adapter version, constructs 1,000 documents before timing, and averages three conversions. At 32/64/128/256 fields, the previous implementation takes 9.528/31.931/93.122/316.413 ms; the indexed implementation takes 4.502/8.508/18.666/36.970 ms. The widest case is about 8.6 times faster. These are local grid-conversion timings, not end-to-end database or UI measurements.
