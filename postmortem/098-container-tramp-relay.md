#+title: Container TRAMP Relay
#+date: 2026-05-19

* Context

Issue #16's original environment was container-centric: a source file was opened
through =/docker:user@container:/path/=, and the database endpoint was visible
from that container network rather than from the local Emacs host.  The first
TRAMP implementation only covered ssh-like methods by mapping them to OpenSSH
=-L=.  That was useful for =/ssh:= and tramp-rpc, but it did not solve the
container case.

* Decision

Keep the connection-origin model and backend contract unchanged, but add a
separate container transport below =:tramp=.

The database backends still connect to a local =127.0.0.1:PORT=.  For container
TRAMP methods, that port is an Emacs TCP listener.  Each accepted client
connection starts =docker exec= or =podman exec= in the TRAMP container and runs
a small stdio relay from inside the container to the configured =:host= and
=:port=.

This supports direct local containers, including OrbStack's Docker-compatible
=docker= CLI, and ssh-like hops to a container runtime, such as
=/ssh:devbox|podman:app:/workspace/=.

* Why This Is Separate From SSH Forwarding

OpenSSH =-L= forwards to a host reachable from an SSH server.  Docker and Podman
TRAMP paths name a container namespace, not an SSH server.  Running the relay
inside that namespace preserves the meaning users expect from a TRAMP source
buffer: =:host "db"= means =db= as resolved by the container, and
=:host "127.0.0.1"= means loopback inside that container.

This also avoids running the database client libraries remotely.  Native
PostgreSQL, native MySQL, and JDBC keep the same local process model; only the
socket path changes.

* Tradeoffs

The container needs one relay-capable command: =socat=, =nc=, =netcat=, or
=bash= with =/dev/tcp=.  That dependency is explicit because there is no
universal Docker or Podman API for attaching an arbitrary TCP stream inside a
container namespace without running some command there.

The relay is intentionally scoped to structured =:host= / =:port= profiles.
SQLite files and raw JDBC =:url= strings are still not rewritten.
