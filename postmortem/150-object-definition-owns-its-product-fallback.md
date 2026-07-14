# 150 — Object Definition Owns Its Product Fallback

## Problem

The composition root exposed `clutch-sql-product` as if it selected SQL syntax
for every workflow.  In practice, connected buffers derive their product from
backend metadata or an explicit connection override.  The option has one
executable use: choosing a mode for an object definition buffer when neither of
those sources is available.

Keeping the option in the root obscured that narrow fallback and made the
assembler own state that only the object workflow understood.  Removing the
public option would be unnecessary breakage, while moving it into connection or
SQL parsing would assign the fallback to code that never consumes it.

## Decision

Keep the public name, default, and Customize type, but define the option in
`clutch-object.el` next to its sole use.  Connected products continue to come
from explicit `:sql-product` values or backend registration; the object option
is consulted only when those sources cannot identify a product.

Update the configuration descriptions to distinguish a connection override
from the object-definition fallback.  No runtime selection behavior changes.

## Consequences

- Mutable state in the composition root falls from two definitions to one.
- Direct object-module loads establish their own fallback without loading the
  package entry point.
- Cross-module declarations remain 43 and the largest dependency SCC remains
  two.
- `clutch-result-max-rows` remains the root's only mutable contract because it
  coordinates fetch, pagination, export, and presentation across workflows.
