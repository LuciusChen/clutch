# 162 — Public Embark Object Contracts

## Context

Point-local object targets used a proper list, which made Embark read the end bound as a one-element list. Clutch also advised private Embark display code to replace command names with action-registry labels.

## Decision

Object finders return Embark's `(TYPE TARGET START . END)` shape. Non-default action labels live in standard labeled keymap entries generated from the existing action registry, and Clutch no longer advises Embark internals. These public contracts keep object resolution, action definition, and presentation separate.

## Consequences

Object bounds remain scalar and non-default action labels remain friendly. Embark currently has no public label field for default-action overrides, so the default action follows Embark's standard command-name display; Clutch will not add a wrapper or private advice solely to rename it.
