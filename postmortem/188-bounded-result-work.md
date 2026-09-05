# 188 - Bounded Result Work and Export Memory

## Evidence

The performance audit measured quadratic Redis array collection and SQL export batch concatenation, 500x120 full renders allocating roughly 16.8 million conses, and unchanged-offset query refreshes retaining previous cell values in the pixel cache. Column sizing scans entire sampled strings before applying its width cap. Local aggregate and filter paths repeatedly walk row/column lists. Reproductions and baselines are preserved outside the repositories in the 2026-09-05 audit artifacts.

## Decision

Keep the current result and protocol boundaries. Use linear collection at the owner, bounded scans for display widths, and one indexed view per aggregate/filter/export operation rather than changing the public row representation.

Cell rendering caches belong to one installed query result. A new result discards value-dependent entries; repeated rendering of that result may reuse them. Character/font measurement caches remain reusable when their shape is compatible. Do not keep prior query values merely because the page offset is unchanged.

Share export batch traversal between clipboard collection and file output. Clipboard output necessarily retains complete text. File output for CSV, TSV and row-oriented INSERT/UPDATE SQL writes each batch to a temporary sibling file and replaces the destination only after all batches succeed. Prompt for the path and coding before doing the expensive fetch. Preserve header, separators, BOM/encoding and the source relation contract. Errors, cancellation and incomplete values remove the temporary file and preserve any existing destination. Native document helper output retains its single-command semantics; it is not a SQL paging path.

Improve rendering by reducing intermediate strings and property copies while retaining existing cell metadata, Unicode widths, selection, row positioning and pixel alignment. Do not change GC thresholds or add view virtualization in this repair. Accept changes only when the same benchmark shows a benefit and the existing rendering contracts pass.

MongoDB encodes each command/document sequence once. Size and batch validation remain protocol-owned and use the encoded bytes that will actually be sent; reducing encoding must not bypass negotiated limits.

## Verification

Use deterministic regression assertions for value retention, ordering, complete output, size rejection, file preservation, and avoidable traversal/allocation. Retain wall-time measurements as reproducible comparative evidence rather than fragile unit-test timeouts. Run standalone protocol suites, real Redis/Mongo commands, Clutch's full checks and native backend suite. Verify graphical cache/render changes in an isolated Emacs session when available.

The repaired workloads passed the full Clutch checks and native backend suite. Separate Redis and MongoDB suites included a real 10,000-element array and multi-document inserts. Terminal and isolated NS graphical sessions compared complete rendered text, properties, headers and row positions against the pre-repair implementation. A direct `char-width` substitution initially changed isolated combining-mark behavior; measuring zero-width marks with the original primitive preserved the buffer-specific result without allocating per-character strings for ordinary text.

The 200,000-row SQLite file export produced identical bytes and SHA-256 with the same 401 queries. The maximum formatted batch fell from 200,000 rows to 500 and process peak RSS fell from about 286 MiB to 129 MiB in fresh processes. Errors, quit, incomplete CLOBs, BOM handling and multibyte encodings have regression coverage. New query refreshes retained only the current 500 cached values after 20 generations instead of all 10,000 historical values. Wide-table rendering reduced cons allocation by about 42%; no product GC settings changed.

This bounds client formatting and historical query-value retention. It does not turn arbitrary backend result materialization into streaming, remove SQL OFFSET costs, or eliminate the full displayed grid and clipboard text. Native document insertMany output remains a single command. These existing boundaries are documented rather than hidden behind a new public data representation.
