# MongoDB metadata reuses background scheduling

## Evidence

Collection refresh and index warmup used synchronous protocol calls, and the field completion idle callback called synchronous `mongodb-find`. Moving that call to idle delayed its start while still holding the Emacs command loop during the network wait. The initial installed-path regression failed on that fallback. A separate regression failed before the shared connection waiter reserved foreground priority, allowing a multi-request metadata series to dispatch again ahead of a waiting user command.

## Ownership

`mongodb.el` owns asynchronous OP_MSG framing, response identifiers, transport deadlines and cursor continuation. Its command callback retains connection ownership until delivery starts, allowing getMore to continue without another request taking the connection. Successful server errors keep a synchronized transport usable. Broken framing, response mismatches, timeout and disconnect retire it. Dispatch failures signal immediately and accepted requests deliver once. Cursor collection shares the existing document budget and int64 command builders, attempts cleanup on known live cursors and preserves the original failure.

Clutch reuses the existing metadata request-series, outcomes, cache generations and connection reservations. The adapter shares sampled-document validation, field profiling and index conversion with synchronous readers. Captured database names keep delayed index results in their request's namespace. The shared background waiter reserves foreground priority until the current wire request returns, giving a waiting user command a turn before the series sends another request. Optional API guards retain the supported older-library path.

## Verification

Protocol tests exercise byte-fragmented replies, wrong identifiers, malformed lengths, command errors, caller callback failure, disconnect, deadlines, dispatch abandonment, cursor continuation and cleanup. The existing real MongoDB metadata fixture compares asynchronous collection, column, detail and index results against synchronous readers. A real failCommand delay verifies concurrent background samples serialize, and delayed index reads verify a foreground ping runs before the next index request. The full native/JDBC runner passed 267 applicable tests across 15 suites with backend-specific skips. It covered PG, MySQL, XTDB, MongoDB, Redis, Oracle, SQL Server, DuckDB and ClickHouse 24.8 using an isolated agent runtime.

The complete non-live gate passed on Emacs 29.4, 30.2 and 32.0.50 with the new protocol library, and again on Emacs 32 with the preceding library. Each run selected 736 main tests (four backend-capability skips), 298 backend tests and 13 architecture tests. Byte compilation, package-lint and checkdoc passed. The protocol library passed all 55 tests and warning-as-error compilation on all three Emacs versions, including real authenticated MongoDB 7 and opt-in timeout/disconnect fault injection. SCRAM-SHA-1 and SCRAM-SHA-256 were both exercised.

## Compiled performance

A disposable OrbStack MongoDB 7 fixture held 1,000 documents with 123 top-level fields and mixed BSON types, yielding 163 sampled field paths. Before and after snapshots used compiled Emacs 32.0.50 code, five runs each locally and with one find reply blocked for 500 ms. Batch execution translated the scheduled idle callback to a regular timer so it exercised the same body. Timing the body matters because synchronous network readers can run other timers while still preventing the command loop from accepting input.

Local mean metadata callback occupancy fell from 9.88 ms to 1.27 ms. With the 500 ms server delay it fell from 511.82 ms to 1.49 ms, with a 1.93 ms maximum after conversion. Mean delayed completion time was 524.34 ms before and 523.47 ms after. Dispatch averaged 0.18 ms afterward and all runs produced the same 163 field paths with zero synchronous find calls. The largest observed delayed heartbeat gap afterward was 5.13 ms. Ten thousand hot-cache checks took approximately 7–25 ms including garbage collection and performed no sampling. These measurements cover this local fixture and controlled delay.

The MongoDB metadata paths share one connection with foreground commands. A user command already waiting on an in-flight metadata reply retains the existing bounded, interruptible admission wait. BSON decoding and field inference still execute on Emacs's main thread, so unusually large samples can occupy it. The sample limit remains the existing control. This PR is stacked on the PG/MySQL metadata PR #226 and requires the new protocol APIs for asynchronous behavior.

All disposable test containers were removed with their volumes, with volume inventory compared against the pre-test baseline.
