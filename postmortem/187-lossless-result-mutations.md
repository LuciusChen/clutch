# 187 - Lossless Result Mutations

## Evidence

An isolated H2 workflow queried `OTHER.AUDIT` and cloned a row through the public clone/stage commands and the atomic submission path. The INSERT targeted the default schema's `AUDIT`, a 1000-character CLOB became its 256-character preview, and the literal text `NULL` became SQL NULL. Independent regressions reproduce each contract failure before changing behavior.

## Decision

Source relation tokens must survive into INSERT as they already do for UPDATE and DELETE. The unqualified table name remains the metadata lookup key; it is not sufficient to identify the SQL mutation target. INSERT copy/export must follow the same relation rule.

Insert fields keep display text separately from whether a value was provided and whether it represents SQL NULL. Typing `NULL` means literal text. `C-c C-n` selects SQL NULL, `C-c C-e` selects an empty string, and `C-c C-d` omits the field so the server supplies its default. Clone and staged-insert reopening preserve explicit NULL and empty strings. Ordinary untouched blank fields retain their existing omission behavior. These states reuse the cell editor's special-value vocabulary rather than parsing magic strings.

The JDBC adapter must not turn an incomplete CLOB preview into a complete string. A generic result-preview value carries its type, original length, and preview text across the result boundary. Viewing remains available, with an explicit preview label; editing, cloning a copied preview field, and exporting that value fail before any write or file output. A preview whose length equals the original length is complete and may be used normally. Full LOB streaming remains outside this repair; refusing a lossy operation is preferable to silently persisting a preview.

The agent's structured BLOB detection may inspect trimmed text, but its returned content must preserve the original whitespace and encoding. This repair does not change the JDBC response schema or the published artifact pin.

## Verification

Regressions cover actual SQLite writes across attached schemas and distinguish literal NULL, SQL NULL, empty strings, and omitted/default fields. JDBC preview tests cover viewing and mutation/export rejection; the isolated H2 workflow confirms that a rejected CLOB clone leaves the original database content unchanged. Protocol encoding and lifecycle regressions remain in their owning standalone libraries.
