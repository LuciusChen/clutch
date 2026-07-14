# 144 — Edit Loads Its Rendering Owner

## Problem

`clutch-edit.el` called 22 functions from `clutch-ui.el`, but represented that
relationship as 22 `declare-function` forms and relied on the composition root
to load UI first.  This duplicated names and signatures without establishing a
runtime dependency.  One duplicated signature had already drifted from the real
function, so byte compilation could not check the caller against its owner.

The declarations also made a stable workflow-to-rendering relationship look
like a lazy boundary.  That obscured the remaining declarations, which are
useful precisely because they identify intentional lazy or cyclic loading.

## Decision

Make the existing dependency explicit: `clutch-edit.el` now requires
`clutch-ui.el` and removes the 22 redundant declarations.  UI does not require
edit, so the edge introduces no cycle.  UI loads only built-in libraries and
the generic backend contract; it does not pull in a protocol implementation or
the package entry point.

The normal entry and result paths already loaded UI before edit.  Only a direct
internal `require` of edit gains the UI definitions it actually needs.

The contributor guide now states the distinction directly: declarations mark
lazy or cyclic boundaries, while a mandatory top-level require supplies the
owner's compile-time contracts.

## Consequences

- Cross-module declarations fall from 65 to 43.
- The largest SCC remains two and root state remains 14.
- Edit can be loaded and byte-compiled independently with the real UI function
  signatures available.
- The remaining declaration metric is more meaningful because it no longer
  counts a mandatory dependency disguised as composition-root wiring.
