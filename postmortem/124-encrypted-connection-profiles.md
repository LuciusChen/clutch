# Encrypted Connection Profiles

## Context

Saved connections often need non-password metadata that can still be sensitive: hostnames, IP addresses, usernames, database names, and SSH tunnel choices. Keeping those fields directly in `clutch-connection-alist` makes local config easy to read, diff, and back up, but it exposes details users may already expect their password store or auth-source files to protect.

## Decision

Clutch now supports `:profile-entry` as a Clutch-level saved-connection key. The named pass or `.authinfo.gpg` profile is read before connection canonicalization, and its fields become defaults for the connection.  Explicit keywords in `clutch-connection-alist` override profile values.

## Rationale

Profiles should reduce config exposure without making saved connections opaque. Treating profile values as defaults keeps the existing plist configuration model intact: users can keep non-sensitive hints such as `:backend` in the alist for completion icons, while moving host/user/database/tunnel details into encrypted storage.  The same override rule also lets one encrypted profile support small variants, such as a different database name or tunnel mode, without duplicating the secret entry.

Pass and `.authinfo.gpg` are both auth-source-adjacent stores users already use for database credentials, so supporting both avoids a Clutch-specific secret format.  `.authinfo.gpg` uses `machine` as the logical profile id and `db-host` for the actual database host because auth-source maps `machine` to `:host`.
