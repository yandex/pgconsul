# Versioned atomic switchover record

- Status: Accepted
- Deciders: munakoiso

# Context

Switchover metadata was split across `master`, `candidate`, `side_replicas`
and `state` ZooKeeper nodes. Readers could observe fields from different
moments. Blocking top-level routing made such torn snapshots actionable on
every host. A command plan may also execute after another host has advanced or
cleaned the operation.

# Decision

Store all cluster-wide switchover metadata in one persistent JSON node,
`switchover/record`. A single ZooKeeper read returns both the complete record
and its version.

Every state-machine write uses compare-and-set with the observed version.
Successful writes update the executor's in-iteration version before the next
command. A version conflict stops the plan and retries from a fresh observation.

Cleanup writes `{}` with compare-and-set instead of deleting the node. The
ZooKeeper version therefore remains monotonic and fences delayed cleanup from a
later switchover. The former multi-node representation is not read or written.

# Alternatives

Use a shared read/write lock around all metadata access. This still requires a
fresh-state check after acquiring the writer lock, and holding a read lock from
planning through database operations would block progress.

Keep separate nodes and publish the phase last. This prevents some incomplete
initialization reads only with a strict reader protocol, but does not protect
multi-field transitions, reset, cleanup, or stale plans.

# Consequences

Readers cannot observe torn switchover metadata. Concurrent or delayed plans
fail their CAS and retry instead of overwriting current state. Cleanup leaves a
small persistent `{}` node. Deployments must not have an active switchover in
the former representation during upgrade.

# Links

- ADR-0005: Idempotent iterations
- ADR-0006: Switchover machine command plans
- ADR-0010: Top-level blocking switchover handler
