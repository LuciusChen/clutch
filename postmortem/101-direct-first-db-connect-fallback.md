# 101 -- Direct-First Requires Database Connection Success

## Background

Postmortem 100 introduced `:ssh-tunnel direct-first` so one saved profile can be
shared by a workstation that needs `:ssh-host` and by a host that can reach the
database directly.

The first implementation treated a successful TCP probe as enough to choose the
direct route. That was too weak for MySQL deployments where the database port
accepts a socket but closes the protocol handshake from a client that is not
allowed to connect directly.

## Decision

`direct-first` keeps the short TCP probe to avoid waiting on a full database
connect timeout when the endpoint is clearly unreachable. When the probe opens,
the direct route is still provisional: only a successful backend connection
selects direct access. Native backends use a short connect/read budget for this
probe, then restore the final connection's normal read-idle timeout when the
direct connection succeeds.

JDBC keeps its database login timeout and Emacs-side agent RPC timeout separate:
the provisional direct connect bounds both, but the normal JDBC path continues
to honor `:rpc-timeout` for the sidecar request.

If the direct backend connection signals `clutch-db-error`, clutch starts the
configured SSH tunnel and retries the connection through the forwarded local
port once.

## Boundaries

The fallback is limited to connection establishment. Query errors, reconnect
after an established connection, and non-`clutch-db-error` programmer failures
continue to surface normally.

The default `:ssh-tunnel always` path is unchanged. Profiles that opt into
`direct-first` pay at most the short TCP probe when the endpoint is not directly
reachable, and at most the short provisional database-connect budget when TCP
accepts a socket but the database route is not actually usable.
