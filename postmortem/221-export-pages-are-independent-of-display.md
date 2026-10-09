# Export pages are independent of display pages

## Decision

SQL exports previously reused `clutch-result-max-rows`, the 500-row display limit. Query export and all-row result export repeat the query for each page, so small pages add queries and can repeatedly scan rows skipped by OFFSET or Oracle ROWNUM wrappers. Export now owns `clutch-export-page-size`, a positive integer defaulting to 2000. Both entry points share the setting and its validation; displayed results retain their existing page size.

The default is a compromise: fewer repeated queries without making the synchronous JDBC remaining-row collection as long as a 5000-row page. Users can choose a larger or smaller value for their row widths and connections. There is no backend-specific tuning, adaptive sizing or new cursor workflow. Larger pages mitigate repeated scans; they do not eliminate them or provide a consistent snapshot.

Explicit SQL limits still run once and formatting splits the returned rows into bounded batches. Cancellation, activity ownership, atomic file replacement, encoding and incomplete-value checks keep the existing implementation. A JDBC page is still collected synchronously after its first batch; fetch requests to the local agent are not a measurement of database network round trips.

## Verification

Existing SQLite command and result-file tests now use different display and export page sizes. Both failed before implementation: direct export executed two queries instead of the required three, and result export passed four rows to a formatter limited to two. The adjusted coverage retains SQL bounds, empty headers, encodings and complete output checks. Existing asynchronous cleanup and flat-stack tests explicitly set the export size so they still cross pages. The 101-column, index-free live fixture sets 200 export rows and 50-row JDBC fetches independently of its 500-row display setting.

The full non-live gate passed on Emacs 29.4, 30.2 and 32.0.50: 704 main tests, 284 backend tests and 13 architecture tests, with one sandbox-only container-forward skip. Byte compilation, package-lint and checkdoc had no warnings. The complete native/JDBC runner passed 15 suites: 245 passing tests and 221 backend capability skips. It used disposable OrbStack databases, ClickHouse 24.8 and the pinned agent 0.2.26 in an isolated runtime; the wide export passed on PostgreSQL, MySQL, Oracle, SQL Server and DuckDB.

Compiled-code probes exported 200,000 rows and eight columns from index-free PostgreSQL 16 and local DuckDB tables. CSV sizes were 28,324,894 and 24,704,894 bytes respectively. Export time excludes reading the completed file for verification. Every row id was checked, and within each backend the files produced with all three page sizes had identical SHA-256 hashes. These are single local measurements on these fixtures, not general performance guarantees or directly comparable to earlier fixtures.

| Export rows per page | PostgreSQL export | DuckDB export |
| --- | --- | --- |
| 500 | 25.48 s | 13.66 s |
| 2000 | 10.98 s | 3.97 s |
| 5000 | 9.01 s | 2.90 s |

Separate disposable Oracle Free and SQL Server 2022 fixtures exported 20,001 rows and eight columns through a localhost proxy adding 25 ms transit delay in each direction. Their three output files also matched byte for byte. Oracle's longest synchronous remaining-row collection was 185 ms with 2000-row pages and 532 ms with 5000; SQL Server's was 21 ms and 43 ms, showing why agent fetch count alone does not determine database round trips. These times measure the synchronous collection call, not GUI input latency; timers can still fire inside its RPC waits. Whole-process peak RSS, including reading the full CSV for verification, stayed between 98 and 160 MiB across the probes and does not establish an export-only memory bound.

Real cancellation of a running PostgreSQL query, Oracle export and SQL Server cross-join export preserved the original file, removed temporary output, ended the activity and left the connection able to execute another SELECT. A fast SQL Server export also exposed an existing cancellation race: if the backend does not accept cancellation, the workflow reports that it is still running and may finish replacing the file. The same probe on compiled `3fc8cf5`, with its display/export page size set to 2000 for comparison, reproduced this at several cancellation times. This is recorded as separate existing work, not counted as successful cancellation or changed by this optimization.

All test containers and their anonymous volumes were removed, the delay proxy and test agents were stopped, and the Docker volume set remained the same 26 volumes. Temporary CSV/database files and isolated driver/runtime copies were removed after verification. No production database, user configuration or installed plugin/runtime was changed.
