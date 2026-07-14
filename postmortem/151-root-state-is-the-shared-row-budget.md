# 151 — Root State Is the Shared Row Budget

## Problem

After workflow-owned state moved out, `clutch-result-max-rows` was the only
mutable definition left in the composition root.  It was still defined after
all workflow modules loaded, so `clutch-query.el` supplied the effective
default first and `clutch-result.el` repeated another declaration.  The metric
said the root owned one value, but neither load order nor the owner guard made
that contract explicit.

Moving the option into query, result, UI, or backend would give one consumer
authority over a policy shared by all of them.  A configuration module created
for one variable would add more glue than it removed.

## Decision

Keep the row budget as the root's sole mutable contract and define it before
assembling workflow modules.  Query uses it to fetch one-row lookahead, result
uses it for trimming, pagination, and export, and UI uses it for global row
positions and footer text.

Retain a documented `defvar` fallback in `clutch-query.el` so intentionally
direct workflow loads keep the same default without reverse-loading the root.
Keep the declaration in the lower-level UI module for special-variable byte
compilation, and remove the redundant result declaration because result already
requires query.

## Consequences

- The architecture budget is fixed at one root state definition and names its
  owner explicitly.
- Normal package loading establishes the shared value before any consumer;
  direct query loading still defaults to 500 rows.
- Cross-module declarations remain 43 and the largest dependency SCC remains
  two.
- Further root-state reduction would require a real shared-configuration
  boundary, not a metric-driven single-variable module.
