# 163 -- Direct-First Uses the Normal Database Timeout

Superseded by [164 -- Connection Entries Own Their Transport](164-explicit-connection-transport.md).

## Background

Postmortem 101 made a direct database login provisional before `:ssh-tunnel direct-first` selected that route. To keep fallback fast, native backends received a hard-coded 0.5-second connect and read-idle deadline, while JDBC received one second. A successful native connection then needed backend-specific code to restore its ordinary read timeout.

That provisional deadline was too close to real connection cost. An Aliyun RDS MySQL login from a host with direct network access repeatedly took about 0.44–0.46 seconds. Small DNS, TLS, network, or scheduler variation could therefore turn a usable direct route into `clutch-db-error`. Clutch would then correctly follow the configured fallback but incorrectly try a workstation-only SSH alias from the bastion host itself.

## Decision

Keep a bounded TCP reachability probe, but replace the hidden 0.25-second constant with the documented `clutch-ssh-direct-first-probe-timeout-seconds` option and a one-second default. The probe returns structured status, elapsed time, and failure reason. Expected `file-error` network failures select SSH and remain visible in debug capture; unexpected internal errors surface instead of being silently reclassified as network failure.

Let the database connection use the profile's normal timeout settings or the backend defaults. A route that cannot accept TCP still falls back promptly. Once TCP accepts, database login gets the same timeout budget it would receive without `direct-first`; only a real connection failure triggers SSH fallback.

Remove the provisional timeout rewrite and the backend timeout-restoration generic and methods. They existed only to compensate for the shortened login deadline.

## Tradeoff

A host that accepts TCP but stalls during database login can now delay SSH fallback until the configured connection timeout. That timeout is already the user's explicit bound for establishing a database session, and preserving it is more predictable than a hidden transport-specific override. Authentication rejection and protocol closure still fall back as soon as the backend reports them.

The one-second probe default makes SSH fallback slower than the old 0.25-second hidden deadline on an unavailable route, but reduces false negatives on slower direct networks and is now explicit user policy. Users who prefer a different reachability/fallback balance can tune the option without changing database login semantics.
