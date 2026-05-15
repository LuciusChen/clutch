#+title: TRAMP-aware Connection Origin
#+date: 2026-05-15

* Context

Issue #16 asked for database connections opened from a TRAMP buffer to reach
databases that are visible only from that remote environment.  A common case is
a SQL file opened inside a remote checkout where the database endpoint is bound
to the remote machine rather than to the local Emacs host.

clutch already had =:ssh-host= for bastion-style access.  That feature starts a
local OpenSSH =-L= forward and keeps the database client local.  TRAMP-aware
connections belong to the same broader "connection origin" problem, but they
are not the same transport.

* Decision

Model SSH and TRAMP as explicit connection transports under one preparation and
cleanup path.

- no explicit transport means local connection
- =:ssh-host= keeps the existing OpenSSH local-forward behavior
- =:tramp-default-directory= maps an ssh-like TRAMP directory to an
  OpenSSH local forward through that host
- =:ssh-host= and =:tramp-default-directory= are mutually exclusive
- command-source TRAMP inference is opt-in through =clutch-tramp-context-policy=

The database backends still receive normal structured =:host "127.0.0.1"= and
=:port LOCAL= params.  The original endpoint and transport are cached on the
connection so UI labels and reconnects keep showing and using the real logical
target.

* Why Not Let Any Open TRAMP Buffer Affect Connections

TRAMP buffers are editing and process contexts, not global connection state.
Using "some TRAMP buffer exists" would make saved connections behave
non-deterministically.  Instead, clutch only considers the buffer that invoked
=clutch-connect= or =clutch-query-console=, and only at connection creation time.

Once the connection is established, follow-up queries, completion, schema
refresh, and reconnect use the connection's stored origin.  They do not re-read
the current buffer's =default-directory=.

* Why Keep Backends Local

The native MySQL/PostgreSQL libraries and the JDBC sidecar already know how to
talk to TCP sockets.  Running those backend implementations remotely would
create separate dependency, installation, and lifecycle problems for each TRAMP
method.

An OpenSSH =-L= forward keeps the backend contract unchanged.  TRAMP only
selects the source host when the current directory is an ssh-like TRAMP path,
which is the part issue #16 needs.

An earlier local-listener design used a TRAMP-started remote stdio relay such
as =nc=, =netcat=, or =socat=.  Live PostgreSQL tests showed that TRAMP async
process stdio is not reliably binary-clean for database wire protocols, so
that bridge is not shipped.

* Tradeoffs

The first version supports structured =:host= / =:port= params only.  Raw JDBC
=:url= strings are not rewritten because that requires driver-specific parsing
and URL mutation rules.

SQLite is excluded because it is a file backend, not a structured TCP
endpoint.  Supporting remote SQLite would need a separate design for opening
and locking a remote database file, or for running a remote sqlite process.
It should not silently share the TCP forwarding path.

The first TRAMP transport version supports ssh-like methods such as
=/ssh:host:/path/=, =/scp:host:/path/=, =/rsync:host:/path/=, and tramp-rpc's
=/rpc:host:/path/=.  Hop chains composed only of those methods are mapped to
OpenSSH =ProxyJump=.  Container TRAMP paths, for example
=/ssh:host|podman:container:/path/=, are rejected with a clear error for now.
Users can still express those topologies with OpenSSH config and =:ssh-host=,
or with a manual tunnel.

This keeps authentication in OpenSSH's batch path.  Plain =/ssh:= TRAMP
sessions that prompted for a password are not reused by the local =ssh -L=
process.  Users need non-interactive OpenSSH authentication or a reusable
ControlMaster.  When tramp-rpc has an active ControlMaster, Clutch may reuse
its ControlPath for =/rpc:= forwards.

The database client's connect timeout applies to the local forwarded port.
Remote connect failures surface through the existing SSH tunnel diagnostics or
through the backend's normal handshake/read timeout path.
