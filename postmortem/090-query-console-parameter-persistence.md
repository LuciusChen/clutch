# Query Console Parameter Persistence

## Problem

Query console contents were persisted by saved connection alias.  That made the alias part of the storage identity: renaming a saved connection caused clutch to open a fresh console file even when the database endpoint, user, schema, and transport were unchanged.

Aliases are display and selection labels.  They are convenient to rename as projects, environments, or naming conventions change.  Treating them as storage identity made a harmless rename look like data loss until the user found the old alias-keyed file.

## Decision

Query consoles now use a stable identity derived from connection parameters:

- backend or JDBC driver
- user
- host and port
- database
- schema
- Oracle SID
- JDBC URL, with password-like URL parameters redacted
- SSH host alias

`clutch--console-name` remains the saved connection alias used for display and reconnect prompts.  `clutch--console-storage-name` is the connection identity hash used to find an already-open console buffer and to select the persistence file. Open-buffer lookup prefers the identity hash.  Alias lookup remains only as a legacy fallback for buffers that do not yet have storage identity state, so a saved alias repointed at a different database does not silently reuse the old connection. When an identity-keyed file does not exist, clutch reads the legacy alias-keyed file.  Subsequent saves use the new identity-keyed file.

## Why This Layer

The query console owns buffer reuse and persistence, so this belongs in the UI layer rather than the backend contract.  Backends already expose the connection parameters needed to build a stable identity, and no protocol operation is required to decide where a local SQL scratch buffer should be saved.

## Rejected Options

### Keep Alias-Keyed Files

Rejected because aliases are mutable labels.  Keeping them as storage identity preserves the rename surprise and makes the user's SQL history depend on naming conventions rather than the database connection being used.

### Rename Legacy Files In Place

Rejected because an alias-keyed file may still be useful if the user has split one old alias into multiple new connections.  Reading the legacy file as a fallback preserves compatibility without deleting or moving user data.

### Store Raw Connection Plists

Rejected because connection plists may contain credentials or irrelevant runtime-only keys.  The storage identity should be deterministic and safe to use as a local file name.  clutch keeps only identity-bearing keys, redacts password-like URL parameters before hashing, and writes only the hash-derived file name.

## Follow-Up Checks

- User documentation should describe persistence by stable connection identity, not by saved connection aliases.
- Internal design docs should list both the alias-facing `clutch--console-name` and the storage-facing `clutch--console-storage-name`.
- Tests should cover alias rename stability for open buffers, legacy alias-file fallback, Oracle SID separation, and URL password redaction.
