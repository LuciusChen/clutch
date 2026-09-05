# Preserve edit cancellation and file/SQL interpretation boundaries

## Evidence

Three independent diff probes fail against the repaired workspace. Opening and cancelling a cloned NULL JSON field clears its explicit state; a real SQLite submission then stores the column default. File export to `.csv.gz` writes plain CSV because the temporary name does not select Emacs's compression handler. A normal `top` column is classified as a row limit, so a real two-row SQLite page returns all three rows. Restoring only the corresponding original functions removes each failure.

## Decision

Opening a JSON sub-editor must only initialize that editor. Keep normalization local until the user saves; the parent field's text and explicit NULL/default/empty state belong to its actual edit/save path.

Keep ordinary file export batched. When the target filename selects a different write handler from the plain temporary file, pass the complete encoded output through that handler once into a second temporary file retaining the target basename suffix. Replace the destination only after transformation succeeds. This preserves handlers that cannot append and avoids repeatedly decoding/re-encoding earlier batches. The handler path may materialize the encoded output in a temporary buffer; ordinary file export retains its bounded formatting and writing. Failures in either phase must preserve the destination and remove both temporary files.

Recognize TOP in the SELECT modifier position with a following count, and FETCH with FIRST/NEXT and a count or ROW/ROWS. Reuse the existing code scanner to ignore nested queries, quoted text and comments, and permit comments between the clause tokens. Keep the established LIMIT/OFFSET checks. This is a bounded lexical correction, not a new SQL parser or dialect framework.

During verification, a non-greedy wildcard inside the repeated comment alternative crossed closing comment delimiters when later tokens did not match. It both misclassified a function call after a comma and caused exponential backtracking across adjacent comments. An isolated comparison confirmed the cause; delimit each comment at its first closing marker, as the existing scanner does, and retain the intervening-code regression alongside a scaling probe.

## Verification

Persist the original failures as repository tests. Confirm actual cloned data after JSON cancellation, and retain existing tests for saving changed JSON and typing after special states. Verify gzip magic and decompressed encoded bytes for UTF-8 BOM, UTF-8 and GB18030, plus normal-file/symlink failure preservation. Check filename transformations run once and cannot replace the destination on error. Pair real SQLite pagination with positive TOP/FETCH cases and negative column/table/alias cases. Run the full Clutch checks and required native live workflows after the focused regressions pass.
