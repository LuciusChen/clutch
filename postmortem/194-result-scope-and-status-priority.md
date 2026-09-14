# Result scope and status priority

## Evidence

Real SQLite queries show that a current-page substring filter feeds its matched-row count into the global page-range formatter. An eight-row query's last page changes from `6-8 of 8` to `6-6 of 6` when only its last row matches. Counting first preserves the total but not the range. Copy follows the selected visible cells, whereas export correctly retains the query's WHERE/LIMIT and ignores the local filter; the export menu does not expose that distinction. Actual 60/80/120-column terminal sessions show long sort labels pushing staged counts or submit/discard hints out of view.

## Decision

Keep page extent and local match count separate. The footer uses loaded page rows for its range and reports the local match fraction explicitly. An empty filtered page explains how to change or clear the filter without inserting fake result cells. No extra COUNT, cross-page client filter, or SQL rewrite is introduced.

Use the existing copy/export menus to name the current-cell/selection and all-result scopes. When a local filter is active, export explicitly says it is ignored. This is presentation only: format, selection, paging, SQL limits, file atomicity, and encoding remain unchanged.

Put transaction state and staged changes before row statistics and sorting. Bound the displayed sort/filter expressions and retain their complete text in help-echo and existing commands. Keep the existing segment caches and native mode-line clipping; no responsive-layout framework, new state, new setting, or connection-identity feature is needed. Extremely narrow windows may still clip hints, but long secondary expressions no longer precede critical state.

## Verification

Require failing regressions for filtered page statistics, empty-state recovery, and priority of staged/transaction indicators before changing implementation. Check actual rendered menus and preserve copy/export output in a real SQLite workflow. Run full non-live checks, native container workflows, and real terminal sessions at the same widths used to reproduce the problem. GUI-specific font/pixel verification is a separate boundary from terminal coverage.

The regressions failed before their fixes and passed afterward. Full checks passed: 571 main tests, 234 backend unit tests, byte compilation, package-lint, checkdoc, and 13 architecture tests. Real SQLite repeated three last-page datasets and preserved query boundaries for unbounded, WHERE-constrained, and explicitly limited exports. At 60, 80, and 120 terminal columns, staged counts and submit/discard hints stayed visible ahead of the long sort label; an actual export menu at 60 columns retained the local-filter warning.

The extended live matrix used Podman PostgreSQL, MySQL, MongoDB, Redis, Oracle, SQL Server, and ClickHouse plus local DuckDB through JDBC: 125 passes, 39 capability-specific skips, no failures. The strengthened local-filter/export workflow passed on all six SQL backends. JDBC used the published 0.2.21 jar verified against Clutch's existing checksum, with drivers isolated from the user's runtime; the agent repository's 254 Java tests also passed. No JDBC source, protocol, release, or checksum changes were required.
