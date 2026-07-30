# 175 — Cell Preview Dwell Instead of Follow

## Context

The optional child-frame cell preview used an 80-millisecond idle timer to coalesce navigation, but changing from one truncated cell to another left the old frame visible until the timer rendered the new value. In a real GUI result grid, the frame therefore appeared to follow point and repeatedly covered the adjacent rows a user was trying to scan.

## Alternatives

A side window would avoid overlay occlusion, but it would resize the query/result layout and recreate the live-viewer workflow removed in 0.3.0. A larger point-to-frame gap or a corner-placement engine would only move the obstruction and add geometry branches without addressing the stale frame shown during navigation.

Corfu's popup-information workflow demonstrates the smaller interaction rule: documentation appears after point rests on a candidate, and implementations may hide the popup while candidates change. Clutch already has the raw cell value locally, so it does not need Corfu's initial/subsequent delay model or its multi-direction placement engine.

## Decision

Treat the automatic cell preview as a dwell preview:

- Hide the existing child frame immediately when point moves to a different truncated cell.
- Show the settled cell after `clutch-cell-preview-delay`, defaulting to 0.25 seconds.
- Keep the existing near-point geometry, content formatting, size limit, lifecycle cleanup, and explicit `v` viewer unchanged.

This removes the visual chasing behavior with one scheduler rule and one user-facing timing value. It does not restore side-window state or duplicate child-frame layout logic.

## Verification

An ERT regression test starts with a visible preview for one cell, moves the scheduled context to another cell, and verifies that the old frame is hidden before the configured timer is installed. The complete cell-preview test group covers coalescing, cleanup, size/position bounds, one-line fitting, and resize handling.

The workspace code was also reloaded into a real graphical Emacs frame. With an exaggerated three-second delay, moving between truncated rows left the grid fully visible during the dwell interval and showed the correctly positioned preview only after the interval elapsed. The delay was then restored to its 0.25-second default and the temporary frame and buffer were removed.
