# 174 — `min-width` Redisplay Version Boundary

## Context

Issue #37 reported that result headers no longer aligned on the supported Emacs 29.4 NS build after postmortem 173 replaced explicit pixel padding with `min-width`. The property and its Lisp APIs exist at the Emacs 29.1 baseline, but API availability does not prove that graphical redisplay enforces the property in every display context.

## Graphical Test

Real NS graphical processes for Emacs 29.1, 29.4, 30.1, 30.2, and a 32.0.50 development build loaded the current `clutch-ui.el` source and rendered the same three-column grid in Menlo 12 with mixed Latin, Chinese, Japanese, and Korean strings. Body runs covered `min-width` directly on left-aligned content and the zero-width carrier used by right-aligned numeric and empty cells; the header used the same carrier. Pixel scanning with `posn-at-x-y` compared successive border positions after redisplay. A second run used the former explicit `space :width` padding as the control.

The target span between borders was 84 pixels. The observed behavior was:

| Emacs | Body `min-width` paths | Header zero-width carrier | Explicit `space :width` control |
| --- | --- | --- | --- |
| 29.1 | Not enforced | Not enforced | 84-pixel spans |
| 29.4 | Not enforced | Not enforced | 84-pixel spans |
| 30.1 | 84-pixel spans | Not enforced | 84-pixel spans |
| 30.2 | 84-pixel spans | Not enforced | 84-pixel spans |
| 32.0.50 | 84-pixel spans | Not enforced | 84-pixel spans |

An attempted zero-width character with a string display replacement plus `min-width` also hung Emacs 29.4, so it is not a safe compatibility technique.

## Repair

Keep the mixed-glyph advantage of redisplay-owned widths only where the tested display path supports it:

- On Emacs 29, render graphical result cells with explicit `space :width` padding.
- On Emacs 30.1 and later, use `min-width` for result-buffer body cells, including direct content widths and the padding carrier used by right-aligned or empty cells.
- Render fixed-width header-line padding with explicit `space :width` on every tested version. This is not an Emacs 29 compatibility branch: the zero-width `min-width` carrier failed through Emacs 32.
- Keep width measurement, logical cell widths, point navigation, and horizontal-scroll behavior shared. Only the final graphical width representation differs.

The implementation follows that split at the final padding representation. Result-body measurement and layout remain shared; an Emacs 29 branch selects explicit display spaces, while Emacs 30 and later select `min-width`. Header centering, sort-indicator normalization, and partial horizontal-scroll padding always use explicit display spaces. Header content is controlled and fixed, and its two-sided centering does not benefit from the mixed-glyph body representation.

## Repair Verification

After the repair, the final workspace was loaded from source into fresh Emacs 29.1 and 29.4 NS processes and rendered in Menlo 12. A three-column header was compared with a body row covering mixed left-aligned text, a right-aligned number, and an empty value. Both releases placed the four header and body borders at pixels `(0 105 210 315)`, producing identical `(105 105 105)` spans. The test also asserted that every header and body cell used explicit display spaces with no `min-width` property. Isolated source-loading ERT checks and native byte compilation passed under both Emacs releases.

A second graphical run reproduced issue #27's short NULL-column case with columns `hb`, `party`, and `rh`, rows containing `nil`, numeric values, and `nil`, and fallback sort indicators. Both Emacs releases computed raw widths `[6 5 6]`, proving the rendered `<null>` placeholder contributed its full six logical cells, then expanded the effective header widths to `[6 7 6]`. Header and body borders matched at `(0 63 133 196)`, with identical `(63 70 63)` spans.

## Removal Condition

When Clutch raises its minimum supported Emacs version from 29.1 to 30.1 or later, delete the Emacs 29 result-body compatibility branch and its tests, leaving `min-width` as the sole graphical body-cell path. Do not delete the header-line explicit-padding path merely because the baseline changes; replace it only after a real graphical test proves a different header representation preserves pixel alignment.

## Testing Lesson

Batch ERT tests that mock `display-graphic-p` or `string-pixel-width` can verify property construction but cannot verify redisplay. Compatibility claims about display specifications need at least one real graphical run that observes rendered positions.
