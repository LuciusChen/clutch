# Document Result Capability Gates

## Context

Native MongoDB results use the same grid renderer as SQL results, but the result menu still exposed SQL-specific affordances: INSERT/UPDATE SQL copy/export, WHERE/ORDER BY rewrites, and staged SQL edit/insert/delete. Runtime guards often rejected those commands, but showing them made native document support look like incomplete SQL support instead of a different data model.

## Decision

Split result actions by capability rather than by backend name.

- Backend-neutral grid actions remain available for SQL and document results: TSV/CSV/Org table copy, value viewing, client-side filtering, aggregate, and visual refine.
- SQL-only actions stay on SQL surfaces: server-side SQL rewrite, staged SQL mutation, SQL INSERT/UPDATE copy, and SQL INSERT/UPDATE export.
- Native document results can expose document mutation payloads only through backend-owned snippet generation.

MongoDB now stores the original source document in a hidden result column. The visible grid still renders sampled top-level fields, while copy/export commands use the hidden source document so they do not reconstruct BSON/JSON from display strings.

## Consequences

Native MongoDB result buffers no longer pretend to support SQL row identity. They provide MongoDB helper snippets such as `insertOne`, `insertMany`, `replaceOne`, `updateOne({ _id }, { $set: ... })`, and `deleteOne` when the source collection and `_id` are available.

This deliberately stops short of a MongoDB staged edit workflow. A real document edit model needs explicit replace-vs-update semantics, BSON extended type handling, array/nested-field behavior, and a commit/preview contract that is not SQL staged mutation under another name.

The generic boundary is still useful for future document databases: the result layer asks whether a document mutation action is supported and delegates payload generation to the adapter. It does not build MongoDB syntax directly.
