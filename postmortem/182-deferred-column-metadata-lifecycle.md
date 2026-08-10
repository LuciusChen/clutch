# 182 — Deferred column metadata stays inside one lifecycle

## Background

Native PostgreSQL, MySQL, and MongoDB completion defers uncached column work to an idle timer. Three lifecycle gaps remained after that move:

- SQL parsing can identify a table name that is not present in the refreshed schema. The deferred callback still queried it and could add an empty entry to the schema cache, turning parser input into invented schema state.
- A failed column load was marked `failed`, but the next completion request immediately submitted it again. A persistent permission or metadata error therefore produced one database request per keystroke.
- Native metadata timers checked foreground and backend busy state, but did not reserve the connection for other metadata timers. Two MongoDB timers could consequently enter one `mongodb.el` client at the same time.

Fail-first ERT reproduced the cache mutation, repeated submission, and reentrant scheduler call. Container-backed PostgreSQL reproduced the unknown-table query through the installed completion-at-point path; the fixed path then passed against both PostgreSQL and MySQL. A MongoDB `failCommand` failpoint made the first real `find` overlap a second metadata request and produced `MongoDB connection is already running a command`; a second failpoint reproduced the repeated request after a real metadata error.

## Decision

`clutch--ensure-columns-async` submits work only when the table is already a key in the current schema cache. A missing schema or missing table is not a metadata candidate. The `failed` state is terminal for that schema snapshot; an explicit schema refresh replaces the snapshot and clears its dependent metadata state before retrying.

`clutch-db--schedule-idle-metadata-call` also takes a per-connection metadata reservation immediately before invoking the backend call and releases it with `unwind-protect`. A competing idle call reschedules through the existing idle retry path. This reservation is separate from foreground ownership and from adapter-specific busy flags because it covers the interval shared by all native idle metadata adapters.

The native-live MongoDB container enables server test commands so the failpoint regressions are deterministic. Production connections and protocol behavior are unchanged.

## Rationale

Querying unknown parser text and then installing its result makes completion a schema-authoring path. Retrying a known failure on each completion request is unbounded and hides the existing refresh recovery action. Adapter-specific locks would duplicate the same scheduler invariant and would still leave a gap for the next native backend.

The shared scheduler reservation keeps all backend calls on the main Emacs thread, preserves the cache-first design, and does not turn errors into empty successes. Explicit refresh remains the single recovery boundary that can replace table membership and retry failed metadata against a new snapshot.

## Verification

Focused tests exercise the public SQL CAPF, real PostgreSQL/MySQL adapter dispatch, the metadata failure state, and a reentrant idle scheduler. The full Podman native-live gate covers real PostgreSQL, MySQL, and MongoDB paths, including the two MongoDB failpoint regressions. The final run deliberately started with an older MongoDB test container that lacked test commands; the runner replaced it before executing the failpoint coverage. The complete non-live, byte-compile, package-lint, checkdoc, and architecture gates pass: 546 main ERT, 222 backend ERT, and 13 architecture tests, with no byte-compile, package-lint, or checkdoc failure. The exact JDBC 0.2.20 Podman matrix passes 122 tests with 39 expected capability skips and no unexpected result; all containers are removed by the runner, and the subsequently published jar matches the checksum now pinned by Clutch.
