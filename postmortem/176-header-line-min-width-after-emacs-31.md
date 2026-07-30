# 176 — Header-Line `min-width` After Emacs 31

## Context

Postmortem 174 correctly retained explicit header padding for the unterminated zero-width carriers tested at the time, but its conclusion that header lines did not enforce `min-width` was broader than the redisplay failure. Later source inspection showed that Emacs renders mode lines, tab lines, and header lines through the same `display_mode_line` path in `src/xdisp.c`, while the tab bar has a separate `display_tab_bar_line` path but still uses the common display-property iterator.

## Upstream Boundary

Two upstream changes establish the reliable version boundary:

- Emacs commit [`71505b723c9fb9de20f6d38be7c73d595e9be3ce`](https://github.com/emacs-mirror/emacs/commit/71505b723c9fb9de20f6d38be7c73d595e9be3ce) (`Fix handling of 'min-width' display property`, bug#72721) passes string positions rather than buffer positions to `display_min_width` and settles display and overlay strings at their real boundaries. It is present on the Emacs 31 branch but not the Emacs 30 branch.
- Emacs commit [`f13e409cf4ff7c1833553b1743d9d224c81ffb98`](https://github.com/emacs-mirror/emacs/commit/f13e409cf4ff7c1833553b1743d9d224c81ffb98) (`Fix min-width in mode-line constructs`, bug#81354) also closes a pending region when a mode-line-like iterator starts a new top-level string. It is included in the `emacs-31.0.91` tag.

Clutch uses Emacs 31.1 as the conservative public boundary rather than recognizing development snapshots whose version strings do not express whether both fixes are present.

## Decision

A header padding carrier is followed by an unpropertized zero-width character before its hidden logical spaces. This explicitly terminates the `min-width` property run; without it, a zero-width carrier immediately followed by a `display ""` run can stall header-line redisplay even on a current Emacs 32 build.

Zero logical width does not guarantee zero graphical width: Menlo 12 on the tested NS build paints the terminator as one pixel. Clutch therefore measures the terminator under the active font and subtracts that width from the carrier's requested minimum. If the terminator is wider than a narrow requested pad, the pad falls back to the existing explicit pixel-space representation.

- Result-buffer body cells remain unchanged: Emacs 29 uses explicit pixel spaces and Emacs 30 or later uses `min-width`.
- Header-line padding remains explicit through Emacs 30 and on the Emacs 31.0.91 prerelease; the public Emacs 31.1 or later path uses terminated `min-width` carriers.
- Logical widths, horizontal scrolling, point navigation, sort-indicator normalization, and header centering remain shared across the graphical representations.

## Verification

Fresh NS graphical processes using Menlo 12 rendered a three-column header and body cells covering mixed left-aligned text, a right-aligned number, and an empty value. Every cell measured exactly 105 pixels, the expected display property was present or absent, and header-line redisplay completed:

| Actual Emacs | Header path | Body path | Header pixels | Body pixels |
| --- | --- | --- | --- | --- |
| 29.1 | explicit | explicit | `(105 105 105)` | `(105 105 105)` |
| 29.4 | explicit | explicit | `(105 105 105)` | `(105 105 105)` |
| 30.1 | explicit | `min-width` | `(105 105 105)` | `(105 105 105)` |
| 30.2 | explicit | `min-width` | `(105 105 105)` | `(105 105 105)` |
| 31.0.91 | explicit | `min-width` | `(105 105 105)` | `(105 105 105)` |
| 31.0.91, Clutch 31.1 branch | `min-width` | `min-width` | `(105 105 105)` | `(105 105 105)` |
| 32.0.50 | `min-width` | `min-width` | `(105 105 105)` | `(105 105 105)` |

Emacs 31.1 was not released at test time. The sixth row uses the real 31.0.91 redisplay engine, which contains both upstream fixes, while binding the version seen by Clutch to 31.1 only while constructing the header. This tests the future branch without identifying the binary itself as Emacs 31.1.

A second graphical run reproduced issue #27's short NULL-column case with columns `hb`, `party`, and `rh`, rows containing `nil`, numeric values, and `nil`, and fallback sort indicators. Every actual binary in the table computed raw widths `[6 5 6]`, proving that the rendered `<null>` placeholder contributed its full six logical cells, then expanded the effective header widths to `[6 7 6]`. Header and body borders both appeared at `(0 42 105 175 238)` including the row-number prefix, so the data-column spans were `(63 70 63)` pixels in every version.

The repository CI passed 529 UI/workflow tests, 205 backend tests, and 13 architecture tests. The padding tests include a font that paints the terminator as one pixel and a zero-padding case that must not add a terminator. With compiler warnings treated as errors, the final `clutch-ui.el` byte-compiled under the actual 29.1, 29.4, 30.1, 30.2, 31.0.91, and 32.0.50 binaries.

## Removal Condition

When Clutch raises its minimum supported Emacs version to 30.1, delete the Emacs 29 result-body compatibility branch and its tests. When the minimum reaches Emacs 31.1, also delete the header explicit-padding branch and its version test.
