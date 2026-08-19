# 178 — Result Header Alignment Under Text Scaling

> Superseded in part by [postmortem 184](184-explicit-result-padding-under-horizontal-scroll.md): non-empty left-aligned result-body cells also use exact pixel padding now.

## Context

Result Browser rows followed `text-scale-mode`, but the header line did not. Setting Emacs's buffer-local `text-scale-remap-header-line` option made the header scale, yet its borders still diverged from the rows because Clutch measured column content outside the result buffer's default-face remappings. The result mode also remapped `mode-line` and `mode-line-inactive` by inheriting `default`; once header-line scaling was enabled, that inheritance could apply the same scale again in the mode-line-like redisplay path used by the header.

Emacs 29.1 `face-remap.el` confirms that `text-scale-mode` remaps `default` and, when `text-scale-remap-header-line` is non-nil, `header-line`. Emacs's `xdisp.c` renders mode lines, tab lines, and header lines through `display_mode_line`; the tab bar enters through `display_tab_bar_line`. In Emacs 29 and 30, `string-pixel-width` accepts only a string and measures it in an internal work buffer, so it cannot inherit the result buffer's face remappings. The optional buffer argument exists in the tested Emacs 31.0.91 and 32.0.50 builds, but Clutch cannot use that call shape while its baseline remains Emacs 29.1.

## Decision

- Set `text-scale-remap-header-line` buffer-locally in `clutch-result-mode`, letting the stock face-remap machinery scale the header with the result body.
- Keep the footer's default background by remapping only its background and box. Do not make mode-line faces inherit `default`, which couples header rendering to a second default-face scale.
- Measure strings after applying the current buffer's relative default-face remappings to the measured copy. This preserves one implementation across the supported version range and keeps the existing newline that settles trailing display specifications.
- Use exact pixel spaces for header centering, right-aligned values, and empty cells. Keep `min-width` only on non-empty left-aligned result-body content from Emacs 30 onward, where it materially improves mixed-glyph layout. Emacs 29 retains exact body padding.
- Reuse the existing pixel-metric signature refresh reached during header evaluation. No new hook, timer, advice, cache layer, or version-specific refresh path is needed.

## Verification

Fresh native NS graphical processes rendered both a mixed Latin/CJK/Japanese/Korean grid and issue #27's `hb` / `party` / `rh` grid containing `<null>`, empty, and numeric values. Each process tested text scales `-2`, `0`, `+1`, and `+3`; every header border matched every body-row border pixel for pixel.

| Actual Emacs | Mixed-grid borders at scale 0 | Issue #27 borders at scale 0 |
| --- | --- | --- |
| 29.1 | `(0 42 91 168 245)` | `(0 42 105 175 238)` |
| 29.4 | `(0 42 91 168 245)` | `(0 42 105 175 238)` |
| 30.2 | `(0 42 91 168 245)` | `(0 42 105 175 238)` |
| 31.0.91 | `(0 42 91 168 245)` | `(0 42 105 175 238)` |
| 32.0.50 | `(0 42 91 168 245)` | `(0 42 105 175 238)` |

The issue #27 fixture retained raw logical widths `[6 5 6]`, confirming that the displayed `<null>` value participates in width calculation. A legacy control using mode-line inheritance failed on Emacs 29.4 at every tested scale, including scale zero, which distinguishes the remap error from ordinary rounding drift. Focused ERT coverage passed under every binary above.

The first Emacs 30 graphical run produced an eager macro-expansion cycle in `map.el` and `comp.el`. That was a test-launcher error: globally forcing `load-suffixes` to `(".el")` made Emacs load its own source files instead of their compiled forms. The corrected launcher copies only Clutch source into a temporary source-only load directory and leaves Emacs's core load policy untouched; no product fallback was added for this harness failure.

## Removal Conditions

Postmortem 184 removed the Emacs 29/30 result-body branch instead of preserving `min-width` on newer versions. When the baseline reaches a public Emacs 31.1 or later, replace the manual default-face remap application with the buffer argument to `string-pixel-width`. Any future padding representation still needs real scaled and horizontally scrolled GUI validation.
