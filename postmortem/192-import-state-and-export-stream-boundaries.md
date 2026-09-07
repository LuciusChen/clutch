# Preserve import state and export stream boundaries

## Evidence

Review of 37041cf found three regressions despite its original fixes passing their tests. A real SQLite clone of a NULL field accepts a single-row CSV import, displays the replacement, but still submits NULL. A 501-row UTF-16 export inserts another BOM at the default 500-row batch boundary. Export through `export.csv.gz` pointing at `payload` writes uncompressed CSV, while the parent version uses the selected filename's compression rule.

The gaps are at existing boundaries: direct field assignment bypasses the new explicit value state; independent encoded writes restart stream signatures; resolving a physical file identity loses the selected filename's transformation semantics. Reverting all of 37041cf would also discard verified data-preservation and bounded-work fixes.

## Decision

Single-row import uses the existing canonical field setter, just like typing and saving an editor. It clears old NULL/default state and updates whether a value is provided. Blank imported cells retain the existing omission behavior; this does not introduce a new CSV NULL/empty representation or change multi-row import.

Keep bounded formatting and writes. The first batch uses the selected coding system. Following batches use its BOM-free UTF-8/UTF-16 counterpart, retaining UTF-16 byte order and the selected EOL conversion. Resolve coding aliases through their base name. Never strip BOM-looking characters from content: an actual U+FEFF in a cell is user data. Other codings keep their current behavior; this repair is not a general stateful codec framework.

Keep the selected absolute path for filename-handler selection and the transformed temporary file's suffix. Keep its physical `file-truename` only for locating sibling temporary files, preserving modes and final replacement. These identities have different responsibilities. This retains chained links and atomic-on-success output without changing the existing complete-output transformation pass for gzip and non-appending handlers.

## Verification

Extend existing tests before changing production code. The SQLite clone test now imports over every original NULL/empty/literal value and checks the actual attached-schema write. The export byte-equivalence fixture includes UTF-16 endian/EOL variants and a literal U+FEFF plus a supplementary character in a later batch. Reuse it for differently suffixed symbolic links in both directions, retaining error/quit/incomplete-value cleanup assertions. Keep existing handler failure, link-chain, paged SQL export and ordinary UTF-8/GB18030 tests.

The prior MySQL EOF, Babel SQLite identity and MongoDB grid changes remain independent local fixes. JDBC stale-handle disconnect and blocking-driver cancellation remain deferred.

## Results (2026-09-06)

All three regressions failed against the pre-fix workspace and pass after the focused changes. The original public-workflow probes now store `replacement` in SQLite, produce gzip magic bytes through the differently named link, and decode the 501-row UTF-16 export with zero internal BOMs. Literal U+FEFF data remains intact in the separate byte-equivalence tests.

Clutch's main/backend suites pass 570 + 235 tests; the native PostgreSQL/MySQL/MongoDB/Redis matrix passes 64 with seven non-selected-backend skips. Checkdoc, required package lint, warning-as-error source compilation and 13 architecture tests pass. Rebuilt straight packages pass 15 targeted tests using their installed bytecode, covering both this repair and the prior three local fixes. Temporary live-test containers were removed; no business database was used. No commits or pushes were made.
