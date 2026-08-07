# 164 -- Connection Entries Own Their Transport

## Background

The `:ssh-tunnel direct-first` mode combined two routes in one saved connection. It first probed the database endpoint, then tried a database login, and could retry through `:ssh-host`. This made route selection depend on timing and made a bastion host attempt a workstation-only SSH alias after a slow or failed direct login.

The database metadata and password still need to be shared between machines. `:profile-entry` already provides that sharing without requiring the connection's transport policy to live in the encrypted profile.

## Decision

Remove `:ssh-tunnel`, the direct TCP probe, and database-connect fallback. `:ssh-host` has one unconditional meaning: start an SSH local forward before the database backend connects. A connection without `:ssh-host` does not try an OpenSSH alias or an automatic SSH fallback; existing explicit and inferred TRAMP forwarding remains unchanged.

Define separate named direct and SSH connections when both routes are useful. They may reference the same `:profile-entry`, so the database endpoint and credentials remain single-source while route choice stays explicit at selection time.

Reject the removed `:ssh-tunnel` key after profile expansion. This catches both connection-alist entries and encrypted profiles instead of silently changing an old direct-first connection into an always-SSH connection.

## Tradeoff

Users choose the route by connection name, and automatic fallback is gone. The alist gains one small entry per route, but no secret or database metadata needs to be duplicated. Connection establishment now makes one backend attempt with the selected route and its ordinary timeout settings.
