# 173 - Redisplay-Owned Minimum Result Cell Widths

> Superseded in part by [postmortem 174](174-min-width-redisplay-version-boundary.md): graphical Emacs 29 needs explicit pixel padding, and header lines cannot use the zero-width `min-width` carrier.

## Context

Postmortem 121 separated logical result-grid widths from graphical pixel widths. Clutch measured visible content with `string-pixel-width`, then represented the remaining width by stretching one padding character and hiding the other logical padding characters. That aligned mixed-font grids, but fixed-width `space` display specifications made Clutch reproduce a decision Emacs redisplay already owns: how much visual space remains after every face, fallback glyph, composition, and `display` specification has taken effect.

Emacs 29 added the `min-width` display specification. It guarantees that a region occupies a requested minimum display width. A trailing `min-width` region is measurable with `string-pixel-width` once the measured string includes a final newline; without that terminator, the last region has not been settled when measurement ends.

## Decision

Keep the logical/pixel split from postmortem 121, but let redisplay enforce every fixed graphical padding width in the result grid:

- Measure strings containing text properties with a final newline so trailing `min-width` and other display state is settled.
- Apply an absolute-pixel `min-width` specification directly to non-empty left-aligned cell content.
- Represent leading, trailing, and two-sided padding with a one-character zero-logical-width `min-width` carrier. Retain the expected number of logical padding spaces behind it with an empty display, so `string-width`, point navigation, column coordinates, terminal behavior, and horizontal-scroll commands keep their established model.
- Use those carriers for numeric leading padding, centered header padding, sort-icon normalization, empty cells, and the visible remainder of a partially cropped header glyph.
- Keep the header pixel-crop path because header lines do not inherit body hscroll. Teach it to reduce a partially visible `min-width` carrier; retain support for cropping an explicit display space supplied by other propertized content.
- Keep `space :align-to` where the header-line display prefix targets a window coordinate. That is positional alignment rather than fixed-width padding and `min-width` is not a substitute for it.

The result-cell cache continues to own content measurement, so adding the terminating newline does not introduce repeated custom-displayer evaluation.

## Compatibility

`min-width`, `add-display-text-property`, and `get-display-property` are available at Clutch's Emacs 29.1 baseline. Terminal rendering still uses ordinary logical padding because graphical pixel layout is disabled there. Numeric alignment, centered headers, column width controls, point restoration, and public configuration are unchanged.
