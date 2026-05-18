#+title: Container TRAMP Origin Inference
#+date: 2026-05-18

* Context

Issue #16 follow-up testing showed that command-source TRAMP inference offered
a =/docker:user@container:/path/= context and then failed during transport
startup.  The user-facing problem was the prompt: Clutch asked permission to
use an origin that the current transport implementation cannot actually use.

* Decision

Source-buffer TRAMP inference now only considers TRAMP methods that the current
OpenSSH forward path can serve.  Unsupported methods such as =/docker:= and
ssh-to-container hop chains are ignored during inference, so saved structured
connections fall back to their normal local interpretation instead of prompting
and then failing.

Explicit =:tramp= / =:tramp-default-directory= values still fail fast for
unsupported TRAMP methods.  Explicit configuration should not be silently
ignored.

* Why Not Treat Docker Like SSH

The shipped TRAMP transport starts a local OpenSSH =-L= forward and keeps the
database client local.  A Docker TRAMP path identifies a container namespace,
not an OpenSSH host that can accept =ssh -L=.  Supporting it would need a
separate container relay: a local listener plus a remote/container-side TCP
client such as =nc=, =socat=, Bash =/dev/tcp=, Python, or a Docker-specific
exec path.

That would change the dependency and diagnostics model, and it would revive the
binary-clean stdio concerns described in the TRAMP-aware connection-origin
record.  It should be designed as a distinct container transport rather than as
a branch inside OpenSSH forwarding.
