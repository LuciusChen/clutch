# 148 — Debug State Belongs to Diagnostics

## Problem

The composition root defined the global debug mode and its dedicated buffer
name even though every effect of enabling the mode belongs to diagnostics.
Modules that already required `clutch-diagnostics.el` repeated declarations for
the mode, leaving the root responsible for state that it only assembled.

Moving the definitions mechanically had two less obvious risks.  A normal
autoload cookie in `clutch-diagnostics.el` would make the public command bypass
the `clutch.el` composition root, while making the JDBC adapter require
diagnostics would create a backend/adapter/diagnostics dependency cycle.  JDBC
can also load before the rest of the package and must still dynamically bind
the debug variables safely.

## Decision

Define `clutch-debug-mode` and `clutch-debug-buffer-name` in
`clutch-diagnostics.el`, next to the capture state and effects they control.
Keep an explicit autoload form targeting `clutch`, so invoking the public mode
still assembles the complete package before dispatching to its owner.

Retain narrow `defvar` fallbacks for the mode and buffer name in
`clutch-db-jdbc.el`.  They support standalone adapter loading without adding a
reverse dependency; the diagnostics owner supplies the real mode and constant
when it loads.  Modules with a mandatory diagnostics require do not repeat
those declarations.

## Consequences

- Mutable state in the composition root falls from eight definitions to six.
- Diagnostics establishes the complete debug contract when loaded directly,
  without loading the root or connection workflow.
- The public `clutch-debug-mode` autoload continues to enter through `clutch`.
- JDBC retains its standalone load order without enlarging the dependency SCC.
