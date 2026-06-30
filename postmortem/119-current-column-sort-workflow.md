# 119 -- Current Column Sort Workflow

## Problem

Result sorting had two overlapping ways to choose a target column:

- Header-line clicks sorted the clicked column directly.
- The `s` command opened a column picker before cycling sort state.

After sorting became a three-state cycle, the picker made `s` feel different
from the header-line workflow.  It also duplicated the existing `C` command,
whose job is already to move point to a named column.

## Decision

Make sorting a current-column action:

- `s` cycles sort for the result column at point.
- Header-line clicks cycle sort for the clicked column.
- `C` remains the way to jump to another visible column before using keyboard
  sort; hidden identity columns are not completion candidates.

The transient label describes the current-column target instead of presenting
sorting as a column-selection command.  It becomes inapt when point is not on a
visible column.  Header clicks treat the column name captured during rendering
as authoritative, so stale header state raises an error instead of sorting a
different column that later occupies the same index.  Headers put original-size
Font Awesome nerd-icons after the column name for the three states; when those
icons are unavailable, the marker is omitted.

## Consequence

Sorting now follows the same spatial model as other result-buffer column
commands such as column info, width adjustment, and aggregation.  Users who want
to sort a far-away column use `C` first or click the header directly; there is no
second sort-specific column picker to keep in sync.
