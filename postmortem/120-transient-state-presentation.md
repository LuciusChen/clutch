# 120 -- Transient State Presentation

## Problem

Clutch transients primarily exposed commands, but several commands also changed
persistent or next-action state.  Their labels described the operation
imperatively, such as "Enable auto-commit" or "Fullscreen", without showing
the current value.  Other menus exposed active filters and staged mutations only
in the result footer, so opening the transient lost useful context.

Copying every `gptel-menu` infix pattern would be the wrong model.  Gptel edits a
request parameter set, while most Clutch entries execute database or navigation
actions immediately.

## Decision

Use gptel-style state presentation only where Clutch already owns a real state:

- Render finite states as parenthesized choices.  The active choice uses
  `transient-value`; inactive choices use `transient-inactive-value`.
- Show compact current values for client and server filters.
- Show staged-mutation counts in the group heading and make staged actions inapt
  when there is nothing staged.
- Make copy refinement, auto-commit, and result layout expose their current
  values.
- Use one shared resolver for the Record action at point, so its dynamic label
  and execution follow the same rules.
- Keep unavailable stateful actions visible but inapt when the surrounding menu
  context remains useful.
- Hide capability-specific groups when none of their actions apply, instead of
  leaving empty headings.

Do not merge actions that are merely opposites or neighbors.  Commit/rollback,
next/previous, widen/narrow, one-shot/live view, and client/SQL filters retain
separate entries because they have different effects rather than representing
one finite setting.

## Consequence

Transient menus now answer both "what can I do?" and "what state am I in?"
without becoming a second settings system.  A small shared formatter owns the
visual state convention; operational commands continue to own state changes and
validation.
