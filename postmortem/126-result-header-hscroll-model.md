# 126 - Result Header Icon and Hscroll Model

## Context

Postmortem 121 introduced graphical pixel padding for mixed font result grids
and made the header-line crop horizontal scrolling by pixels. That keeps a
partially scrolled display-space padding character alive with its remaining
width instead of dropping it as a whole logical character.

While adding plain-text sort-indicator fallbacks, it was tempting to simplify
the header-line by cropping with `truncate-string-to-width` only. That reverts
the old drift: body rows are rendered by Emacs from strings containing display
spaces, while the header-line is a separate string that must be cropped in the
same displayed-pixel model when graphical pixel padding is active.

Separately, `nerd-icons` sort glyphs can have a rendered pixel width that does
not match their logical `string-width`. Repeated sort indicators in wide tables
can then accumulate header/body drift unless the icon enters the table model as
an integral cell-width string.

## Decision

Keep the pixel-crop path from postmortem 121 for graphical result headers when
pixel column metrics are active. Logical cropping remains the fallback for
terminal display and non-pixel layouts.

Sort indicators in result headers keep using `nerd-icons` when available and
fall back to plain text (`↕`, `↑`, `↓`) when icons are unavailable. The
`nerd-icons` glyphs are normalized to integral logical cells before entering the
header label, so the existing pixel layout and hscroll code see a stable table
cell instead of a raw proportional icon.

Header hscroll still crops strings in the UI layer because header-lines do not
inherit the buffer body's horizontal scroll.  When the crop point lands inside a
display-space padding character, the padding is narrowed.  When it lands inside
an ordinary glyph such as a fallback arrow or `nerd-icons` glyph, the glyph
cannot be partially represented as a string; Clutch drops that glyph and inserts
a zero-logical-width display-space for the clipped remainder so following column
borders keep their pixel positions.

## Compatibility

This keeps terminal behavior unchanged. The graphical path still has an
explicit pixel-crop branch, but that branch is the ownership boundary for
header-line display strings; removing it makes the header use a different
display model from the body.
