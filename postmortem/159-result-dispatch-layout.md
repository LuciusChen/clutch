# 159 — Result Dispatch Layout

## Context

The result dispatch exposed every command, but its three uneven rows mixed movement with inspection, split staged mutations from mutation commands, and kept pending-only actions visibly disabled. The resulting empty space and duplicate workflow headings made the menu harder to scan than the result-mode keymap itself.

## Decision

Keep every command and key unchanged, but use two rows of four workflow groups. The first row contains Navigate, Pages, Query, and Filter / Sort. The second contains Edit, Inspect, Copy / Export, and Layout. Move record/value/column details into Inspect, keep every staged-mutation action in Edit, and move horizontal scrolling from data pagination to Layout. Pending-only suffixes appear only while pending changes exist.

## Why

The menu is a discoverability surface, not a second command hierarchy. Grouping by the user's immediate task makes the existing keys easier to find without adding submenus, aliases, state, or helper abstractions. Compact key spacing avoids letting long chord names determine every column's width. Hiding pending-only suffixes removes inactive noise while their owning workflow remains visible.

## Consequence

Result commands, bindings, predicates, and execution behavior remain unchanged. The standalone Staged group disappears, while its useful pending count and exact staged-SQL actions move to Edit without conflating staged mutations with result-set serialization in Copy / Export.
