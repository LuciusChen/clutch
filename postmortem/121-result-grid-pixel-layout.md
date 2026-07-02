# 121 - Result Grid Pixel Layout

## Context

The result grid used `string-width` for both buffer layout and visual column
alignment. That works when every displayed glyph follows the frame's canonical
cell width, but mixed ASCII/CJK font fallback can produce different pixel
ratios. Header-line borders then drift from body borders even though both strings
have the same logical width.

## Decision

Keep logical and graphical widths separate:

- `string-width` remains authoritative for truncation, navigation, horizontal
  scrolling, terminal display, and user-controlled column widths.
- On graphical displays, measure the rendered header and current-page cell
  contents with `string-pixel-width` and keep one maximum pixel target per
  column.
- Pad header and body cells to the shared target with `display` space
  properties. When a full logical-width value still needs pixel padding, use a
  zero-logical-width carrier so buffer columns do not change.
- Cache measured cell content in the render state so custom display functions
  are evaluated once per render.
- Render in the displayed result window when one exists. `string-pixel-width`
  and `default-font-width` depend on the selected frame/window, so rendering a
  visible result buffer from another selected window can measure the wrong font.
- Store the graphical font metric signature used for the layout. Header-line
  redisplay schedules a coalesced redraw when buffer-local face remapping or
  the measured default/CJK sample changes.
- Crop header-line horizontal scrolling by pixels on graphical displays so a
  partially scrolled `display` space keeps its remaining width instead of being
  dropped as a whole logical character.

Incremental row redraw measures only the changed row against the existing pixel
targets. It falls back to a full redraw only when that row makes a column wider.

## Compatibility

This changes only graphical presentation. Existing column-width state, commands,
point restoration, text properties, terminal output, and public configuration
remain unchanged.
