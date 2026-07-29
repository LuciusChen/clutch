# 172 — TSV Export and Header Control

## Context

Clutch already copies the current cell or selected rectangle as tab-separated text through `c`, while `e` serializes the complete result, auto-paging pageable queries beyond the displayed row limit. Issue #36 exposed the missing combination: CSV had full-result clipboard and file targets, but TSV existed only in the selection-oriented copy workflow and omitted column headers.

## Decision

Add TSV to the full-result export kinds and expose it through the Export Transient. Like CSV export, it serializes every collected row for either clipboard or file destinations. File export uses the same explicit encoding choices and UTF-8-with-BOM default as CSV for spreadsheet compatibility.

CSV and TSV share one delimiter-aware serializer. A field is quoted when it contains its format's delimiter, a double quote, or a line break, and embedded double quotes are doubled. This keeps either format structurally valid without maintaining two escaping rules.

Make column headers an explicit choice throughout tabular serialization. The Copy and Export Transients both default Header to Yes, preserving the existing CSV/Org default while making TSV consistent; users can switch it to No. Export also presents clipboard/file as a Destination option, so format, destination, and header policy are visible before execution instead of being spread across sequential completion prompts.

## Scope

Keep `c` selection-oriented and `e` full-result-oriented, but give both entry points the same Transient interaction model. Export formats remain capability-aware: SQL mutation formats appear only for relational results, document helpers only where supported, and CSV/TSV remain available across result surfaces.

## Why

Copy and export answer different questions. Copy operates on an explicit local selection; export represents the result set and may retrieve rows not present on the current page. They should nevertheless use the same tabular serialization rules, including an explicit header choice. Unifying TSV with the existing row serializer removes its one-off cell-text path instead of stacking another conditional onto it.
