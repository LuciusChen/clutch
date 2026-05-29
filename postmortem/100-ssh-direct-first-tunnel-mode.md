# 100 -- SSH Tunnel Mode Belongs to the Connection Profile

Superseded by
[`101 -- Direct-First Requires Database Connection Success`](101-direct-first-db-connect-fallback.md).

## Background

`:ssh-host` originally had one clear meaning: build an OpenSSH local forward
through the named host before the database backend connects. That works well
for a workstation reaching a database through a bastion, but it becomes awkward
when the same `clutch-connection-alist` is shared with the bastion host itself.

For example, a Mac may need `:ssh-host "arch"` to reach an RDS endpoint, while
Emacs running on `arch` can reach the same RDS host directly. Keeping
`:ssh-host "arch"` in the shared profile made the `arch` session try to run
`ssh arch`, which depends on local SSH alias setup and adds an unnecessary
self-hop.

## Decision

Clutch keeps `:ssh-host` as an explicit tunnel request by default and adds an
opt-in profile key:

```elisp
:ssh-tunnel direct-first
```

With this mode, clutch briefly probes the configured `:host` / `:port` TCP
endpoint first. If the endpoint is reachable, the backend connects directly. If
the endpoint is not reachable, clutch starts the normal `:ssh-host` tunnel and
connects through the forwarded local port.

The default remains equivalent to:

```elisp
:ssh-tunnel always
```

## Why Not Change `:ssh-host`

Changing `:ssh-host` to direct-first by default would make a previously explicit
transport directive conditional. That would be a user-visible behavior change
for users who intentionally route through a bastion for audit, firewall, or
source-IP reasons even when the database port is technically reachable.

Clutch should not infer that direct TCP reachability means direct database
access is desired.

## Why Not Detect the Local Machine

OpenSSH aliases are not reliable machine identities. `Host arch` may expand to
an IP address, a DNS name, a ProxyJump chain, or a host whose actual
`system-name` differs from the alias. Parsing `~/.ssh/config` or asking users
to maintain local host identifier lists would move environment-specific
identity logic into clutch.

The profile-level mode instead describes the desired access strategy: use the
direct endpoint when it is reachable, otherwise fall back to the configured SSH
host.

## Limits

The direct-first probe is TCP reachability only. It does not attempt database
authentication before falling back, so bad credentials or database-level
authorization failures surface on the selected route instead of trying a second
login path.

This keeps the behavior predictable and avoids mixing connection routing with
database authentication semantics.
