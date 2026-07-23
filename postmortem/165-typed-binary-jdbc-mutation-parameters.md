# 165 - Typed Binary JDBC Mutation Parameters

> Superseded in part by [167](167-jdbc-blob-data-interface-release.md): non-empty BLOB values now use the JDBC binary data interface, while a non-null zero-byte BLOB uses an explicitly managed empty `Blob`.

## Context

JDBC staged mutations already preserved the prepared-statement boundary, but Clutch discarded each parameter's backend type before sending `execute-params`, and the agent bound the resulting value with `PreparedStatement.setObject`. That is sufficient for ordinary scalar and structured values, but not for Oracle BLOB writes: a text edit reached the driver as a Java string, Oracle attempted a hexadecimal conversion for the binary column, and the update failed with `ORA-01465: invalid hex number`.

The type was not actually unknowable. JDBC result metadata and `get-columns` already returned the declared type, while Clutch's shared parameter object already had a backend-type slot. The loss happened at two explicit boundaries: normalized JDBC columns did not retain the exact type as `:backend-type`, and the JDBC adapter stripped the type before RPC serialization.

## Decision

Retain exact JDBC type names on result columns and column details, then use that metadata only for parameters classified as binary. Clutch sends those parameters through this reserved envelope:

```json
{"__clutch_jdbc_param":"binary","jdbc-type":"BLOB","base64":"eyJzdGF0dXMiOiJyZWFkeSJ9"}
```

`__clutch_jdbc_param` must be `binary`. `jdbc-type` is the exact declared JDBC type. `base64` transports the exact bytes without asking JSON or the JDBC driver to interpret a text representation.

The value semantics are explicit:

- A base64 string carries a non-null byte sequence, and `""` carries zero bytes.
- A JSON `null` `base64` carries typed SQL NULL.
- The agent binds BLOB types with `PreparedStatement.setBlob`.
- The agent binds RAW/BINARY-family types with `PreparedStatement.setBytes`.
- Unsupported types and malformed envelopes fail before JDBC execution.

Ordinary parameters remain ordinary JSON values and keep the existing `setObject` path. This is not a general JDBC type-tag protocol.

## Encoding Ownership

Text-like BLOB results may include the encoding used by the agent's strict decoder. Clutch retains that encoding as value metadata, carries it through the cell-edit buffer, and uses it when converting the edited text back to bytes. A new textual binary value without source encoding uses UTF-8. Unibyte strings and byte vectors already represent bytes and are transported unchanged before base64 encoding.

This ownership split keeps UI/edit state in Clutch and JDBC binding in the agent. The agent receives bytes and a declared binary kind; it does not infer text encodings.

## Why Not Other Approaches

Rendering Oracle hexadecimal literals would discard the prepared-parameter boundary, introduce dialect-specific SQL, and duplicate escaping rules. Continuing to call `setObject` with a string reproduces the failure. Adding a type tag to every JDBC value would claim portability that drivers do not provide. Treating every edited BLOB as UTF-8 would corrupt text that the read path decoded as GB18030 or another explicitly reported supported encoding.

A reserved binary envelope is the narrowest contract that preserves known metadata and exact bytes. It also avoids confusing a normal JSON object parameter with protocol control data: only an object carrying the reserved marker enters the binary path.

## Compatibility and Release Boundary

Maps, arrays, scalars, booleans, numbers, strings, and SQL NULL without the reserved marker remain wire-compatible with the existing prepared-value protocol. Binary envelopes require the matching agent release, so the agent version and published-jar checksum move together before Clutch advertises the new pin.

This change does not add full binary BLOB fetching, BLOB streaming, arbitrary JDBC type coercion, or binary-content editing beyond values already available to Clutch's staged mutation workflow.

## Superseded Decision

This decision supersedes the values-only wire-format conclusion in [128](128-jdbc-prepared-mutation-execution.md) for binary parameters. The earlier separation between rendered preview SQL and parameterized execution remains unchanged.
