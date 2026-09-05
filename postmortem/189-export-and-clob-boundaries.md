# Preserve CLOB length units and export link targets

## Evidence

The lossless CLOB guard from 187 compared Emacs character counts with JDBC lengths. A real H2 query through the published 0.2.20 agent returns the complete CLOB `😀` with length 2, while Emacs counts one character. The guard incorrectly blocks this complete value. A complete 128-emoji value at the 256-unit preview boundary reproduces the same failure.

The batched export from 188 replaced the selected path by renaming a temporary file. When that path was a symbolic link, the export replaced the link itself and left its referent unchanged. The original direct `write-region` followed the link. Both regressions fail focused tests and pass when the corresponding original functions are restored.

## Decision

Normalize CLOB lengths in the JDBC adapter using UTF-16 code units: supplementary code points count twice. Keep the exact completeness comparison and incomplete-value protection. Count the bounded preview directly without allocating another encoded string; full CLOB streaming remains unsupported.

Expanded H2 verification also exposed an existing agent bug: cutting after a high surrogate produces JSON that Emacs rejects before normalization. The agent owns that truncation boundary and now backs off one UTF-16 unit, retaining the original length. See agent postmortem 025. This producer fix is verified using a locally built jar; the published 0.2.20 artifact and Clutch's pin are unchanged pending a release.

Resolve the selected export path with `file-truename` before creating the temporary file. Create that file beside the resolved destination, preserve the destination's modes, and replace it only after every batch succeeds. Keep the user-selected path for messages. This preserves relative and chained links, including links whose final target does not exist yet, while retaining bounded formatting and cleanup on error or quit.

## Verification

Regression tests cover complete BMP and supplementary text, the 256 UTF-16-unit boundary and longer values, and export through relative link chains to existing and absent targets. Success must update the referent without altering either link; errors, quit and incomplete values must preserve its previous contents or absence, permissions, and leave no temporary files. Existing byte-exact export encoding, BOM and paged SQLite tests remain applicable. Confirm the original reproductions through the published JDBC agent and the actual file export entry point after repair.
