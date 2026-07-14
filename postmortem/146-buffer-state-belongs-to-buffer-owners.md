# 146 — Buffer State Belongs to Buffer Owners

## Problem

The composition root still defined three buffer-local workflow variables.  The
last query and last result buffer are written by query execution and consumed
after result has already required query.  The base query is created and mutated
only by result filtering and pagination.  Their definitions in `clutch.el`
gave the root lifecycle responsibility for state it never read or wrote.

Sibling modules also carried eight forward declarations for these variables.
Three stood where the owner definitions belonged, two were covered by an
existing mandatory require, and three had no production references at all.
This was leftover assembly glue rather than a useful lazy boundary.

## Decision

Define the last-query and last-result-buffer variables as buffer-local state in
`clutch-query.el`.  Define the base query as buffer-local state in
`clutch-result.el`.  Remove the root definitions and all redundant or unused
sibling declarations.

Keeping the real definitions as `defvar-local` is important: query code uses
ordinary `setq` in several execution paths and relies on the symbols already
being permanently local in query buffers.

## Consequences

- Mutable state in the composition root falls from 11 definitions to eight.
- Cross-module declarations remain 43 and the largest SCC remains two.
- Query and result buffers retain the same local values and reset behavior.
- Direct module loads now establish their own buffer-state semantics without a
  later `require` of the package entry point.
