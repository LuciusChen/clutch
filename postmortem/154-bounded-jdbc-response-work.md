# 154 - Bound JDBC response work without a request framework

## Context

The JDBC process filter searched for a newline from the start of its buffer on
every fragment. A large response arriving in small chunks therefore rescanned
the same incomplete JSON repeatedly. At 4 KiB fragments, a synthetic 2 MiB
response took about 300 ms to collect.

Timeout and disconnect paths had two related lifecycle gaps: late responses
could enter the shared response queue, and a callback already deferred onto a
zero-delay timer could run after its connection was gone.

## Decision

Keep the existing newline protocol and request tables. The filter now remembers
the end of the already-scanned fragment in its process buffer; completed lines
are still parsed and deleted exactly as before. Timed-out request ids are marked
for removal when their late response arrives, connection-scoped callbacks are
cancelled on disconnect, and deferred callbacks verify that their connection is
still the registered instance before running.

This kept the change local. The same 2 MiB response now takes about 15.6 ms, and
the measured 1-to-2 MiB growth is close to linear rather than quadratic.

## Deliberate limit

This change does not add a second active-request registry or connection
generation framework. An ignored id can remain recorded if its response never
arrives, although a shared-agent restart clears that state. A reentrant direct
disconnect while a synchronous waiter is still active also remains ambiguous:
dropping that response would force the waiter to time out and kill the shared
agent, while retaining it is necessary for the waiter to finish.

Those cases need one unified request-lifecycle design, not an arbitrary
tombstone cap or another timer table. Add that machinery only after a real
failure or measurement justifies its state and invalidation cost.
