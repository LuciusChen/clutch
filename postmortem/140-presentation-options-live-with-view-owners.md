# 140 — Presentation Options Live With View Owners

## Problem

The composition root still defined eight presentation and diagnostics options
that each had one implementation owner.  Result, UI, and diagnostics declared
those variables locally but depended on root assembly for their Custom
metadata.  One query fallback also repeated a result-window default it never
read.

As with earlier workflow options, this split hid ownership from the dependency
graph and let root state remain large even after implementation cycles were
reduced.

## Decision

- Result owns its window height, external-agent truncation limits, column-width
  adjustment step, and CSV coding default.
- UI owns the maximum rendered column width and cell padding.
- Diagnostics owns the retained debug-event limit.

The public names, defaults, types, groups, and documentation moved verbatim.
The unused query fallback and the eight owner declarations were removed.  No
require, autoload, alias, accessor, or configuration facade was added.

The shared result-row limit was deliberately excluded: query fetches with it,
result pages with it, and UI renders against it.  A convenient file location
would not establish a single policy owner.

## Architecture and Test Budget

The root-state ceiling fell from 22 definitions to 14.  The existing owner
table gained three rows and remains the only source-owner test.  The existing
standalone workflow-load test now also requires result; no new ERT case or
subprocess fixture was introduced.

## Consequences

- `clutch.el` fell from 345 lines to 299.
- Production source is seven lines smaller after removing duplicate fallbacks.
- Cross-module declarations remain 134 and the largest SCC remains two.
- Direct result, UI, and diagnostics loads establish the Custom definitions
  they consume without loading the composition root.

## Deferred Boundary

The remaining root state is shared query/result lifecycle, backend timeout
policy, the cross-workflow result-row limit, SQL product fallback, and the
public debug-mode workflow.  Debug mode must move with its autoload target and
lazy JDBC contract, not as another mechanical variable relocation.
