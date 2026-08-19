# 184 — Explicit Result Padding Under Horizontal Scroll

## Context

[Issue #41](https://github.com/LuciusChen/clutch/issues/41) reported that some Result Browser columns became misaligned after `C` selected a column or Tab paged horizontally. Pressing `[` or `]` repaired the display. The failure appeared most readily after a page with wide values established large graphical column targets and a later refresh replaced them with short values.

The result header and body are separate redisplay layers. Emacs clips the buffer body according to `window-hscroll`, while Clutch crops its header string manually by the same logical scroll count. Since postmortem 174, Emacs 30 and later represented non-empty left-aligned body widths with `min-width`; headers, right-aligned cells, and empty cells used explicit pixel-width spaces.

## Reproduction

A controlled fixture first rendered wide values, refreshed with short values while retaining the wider pixel targets, and then used `C` to select a later column. The logical column widths and graphical targets were identical in both runs. `C` selected `window-hscroll` 54, a position inside the logical `delta` cell rather than on a column border.

On the same Emacs 32 redisplay engine, forcing the Emacs 29 explicit-padding representation kept header and body aligned at hscroll 54; the Emacs 30+ `min-width` representation did not. Both representations had the same pixel-width vector:

```text
[56 70 77 140 63 70 84 84 84 84 84 84]
```

Native GUI checks reproduced the split on Emacs 29.4 with explicit padding and Emacs 30.2 and 32.0.50 with `min-width`. Ten Tab presses produced the same distinction: the `min-width` grid became misaligned, while explicit padding remained aligned. `[` and `]` repaired the former by snapping horizontal scroll to a column border.

Replacing every NULL value in the fixture with the ordinary string `"x"` still reproduced the Emacs 30.2 failure. NULL rendering, nil semantics, and the italic NULL face were not causal.

## Root Cause

The logical padding retained behind a `min-width` content run preserved buffer coordinates, but it did not make body clipping equivalent to cropping an explicit header string when hscroll began inside that run. The earlier tests proved stationary column spans and border-aligned scrolling; they did not test an interior scroll position after a pixel target outgrew the current content.

`clutch--center-column-in-window` and Tab paging may legitimately choose such interior positions. Treating `[` and `]` as the only safe paths would leave two existing navigation commands able to corrupt the grid's visual contract.

## Decision

- Represent graphical result-body padding with explicit pixel-width spaces on every supported Emacs version, matching the header representation.
- Retain logical padding characters behind those display spaces, so point navigation, terminal layout, and horizontal-scroll coordinates keep the established model.
- Keep measuring the actual rendered content, including custom displayers, fallback glyphs, compositions, and default-face remapping. Only the final padding representation changes.
- Measure propertized content returned by custom displayers at its actual rendered width instead of stripping its `display` properties. Clutch no longer generates `min-width` for its own body padding.
- Do not force `C` or Tab to column borders. Interior horizontal positions are valid and the grid representation must render them correctly.

## Verification

The padding contract now requires left-aligned, right-aligned, and empty cells to reach the target pixel width without generated `min-width`. The mixed-width custom-displayer test verifies the same representation through the full result renderer. Both tests failed against the previous Emacs 30+ branch and passed after the branch was removed.

An Emacs 30.2 GUI matrix rendered Latin, Chinese, Japanese, Korean, custom `display` properties, right-aligned numbers, empty strings, and NULL values at text scales -2, 0, +1, and +3. Header and body borders matched at every scale. At scale +3, both a `C` jump and Tab paging produced non-border hscroll values and remained aligned. The same explicit-padding control aligned on Emacs 29.4 and 32.0.50. After the change, the current source was loaded into an Emacs 32.0.50 GUI; both a mid-cell `C` jump and repeated Tab paging remained aligned.

The focused padding and custom-displayer tests passed from source under Emacs 29.4 and 30.2. The complete repository check passed 551 UI/workflow tests, 229 backend tests, 13 architecture tests, byte compilation with warnings as errors, package-lint, and checkdoc.

## Testing Lesson

Rendered border equality at hscroll zero is not enough for a horizontally scrollable grid whose header and body use different redisplay paths. A graphical compatibility fixture must also retain a wider previous target, scroll to a logical position inside a cell, and compare the layers there.
