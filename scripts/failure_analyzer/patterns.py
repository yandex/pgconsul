"""Known failure patterns and their diagnostic weights.

Patterns are declared as plain ``(regex, name, weight)`` tuples and compiled
once into :class:`Pattern` objects at import time. The compiled regex is reused
on every scanned line, which removes the per-line ``re.compile`` cost of the
original implementation.

Higher ``weight`` means "more likely to be the root cause".
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class Pattern:
    """A compiled failure pattern with a human-readable name and weight."""

    regex: str
    name: str
    weight: int
    compiled: "re.Pattern[str]"

    @classmethod
    def from_tuple(cls, raw: tuple[str, str, int]) -> "Pattern":
        regex, name, weight = raw
        return cls(
            regex=regex,
            name=name,
            weight=weight,
            compiled=re.compile(regex, re.IGNORECASE),
        )

    def search(self, line: str) -> bool:
        return self.compiled.search(line) is not None


@dataclass(frozen=True)
class StuckPattern:
    """A pattern that indicates a stuck/looping state (no weight)."""

    regex: str
    name: str
    compiled: "re.Pattern[str]"

    @classmethod
    def from_tuple(cls, raw: tuple[str, str]) -> "StuckPattern":
        regex, name = raw
        return cls(regex=regex, name=name, compiled=re.compile(regex, re.IGNORECASE))

    def search(self, line: str) -> bool:
        return self.compiled.search(line) is not None


# ---------------------------------------------------------------------------
# Known failure patterns — ordered by diagnostic priority
# ---------------------------------------------------------------------------

# Patterns are (regex, human-readable name, severity weight)
# Higher weight = more likely to be the root cause.
_PGCONSUL_RAW: list[tuple[str, str, int]] = [
    (r'requested starting point .* is ahead of the WAL flush position',
     'WAL divergence: replica LSN behind new primary (needs pg_rewind)', 100),
    (r'could not receive data from WAL stream.*requested starting point',
     'WAL stream rejected: starting point ahead of flush position', 95),
    # Switchover-specific failures (high priority — often the real root cause)
    (r'SWITCHOVER PHASE.*failed|TransitionTo.*FAILED.*switchover',
     'Switchover transitioned to FAILED state', 98),
    # Missing "SWITCHOVER STARTED" event — lost during ADR-0006 migration.
    # The candidate machine (plan_initiated) must emit this Log event so
    # behave tests asserting "SWITCHOVER STARTED" in order can pass.
    (r'SWITCHOVER STARTED',
     'SWITCHOVER STARTED event present (switchover entry point logged)', 5),
    # Primary detected CANDIDATE_FOUND in plan_initiated. If this log appears
    # but is NOT followed by "Cluster closed from user requests (pooler stopped)"
    # in the same iteration, the primary wasted an iteration deferring StopPooler
    # to plan_candidate_found — the root cause of pgconsul_util.feature:402
    # --block timeout (extra ~4s pushes total past 60s).
    (r'candidate_found detected, proceeding to shutdown',
     'Primary detected CANDIDATE_FOUND in plan_initiated (check if pooler stop follows in same iteration — extra iteration causes --block timeout)', 40),
    # Pooler stopped log from plan_candidate_found — correlates with the
    # candidate_found detection above. If these two are in different iterations,
    # the primary wasted an iteration (pgconsul_util.feature:402 root cause).
    (r'Cluster closed from user requests \(pooler stopped\)',
     'Pooler stopped (correlate with candidate_found detection — same iteration = no wasted iteration)', 10),
    (r'Switchover sync_set: candidate is None, aborting',
     'Switchover aborted: candidate not written to ZK before SYNC_SET (anywhere-switchover bug)', 97),
    (r'Switchover (initiated|candidate_found|pooler_stopped|primary_shut): candidate is None',
     'Switchover aborted: candidate is None in mid-phase', 96),
    # Candidate has no handler for a switchover phase — missing planner in
    # CandidateSwitchoverMachine (e.g. pg_stopped was unhandled, see ADR-0006).
    # NOTE: 'scheduled' and 'sync_set' are intentionally unhandled on the
    # candidate side (candidate is not yet involved); only mid-shutdown phases
    # (pooler_stopped, pg_stopped, etc.) represent real missing-planner bugs.
    (r'No candidate-side planner for switchover phase (?!scheduled|sync_set)',
     'Candidate has no handler for switchover phase (missing planner in CandidateSwitchoverMachine)', 93),
    # Failed promote — candidate cannot promote itself, switchover stuck in
    # a promote-retry loop (no transition to FAILED state).
    (r'Could not promote me as a new primary',
     'Failed promote: candidate could not promote (switchover stuck in promote loop)', 92),
    # Failover blocked: after a failed switchover, the old primary rewound to the
    # ex-candidate (which also failed to promote) and is now a replica. It tries
    # failover but is_promote_safe() returns False (no sync replica caught up),
    # so promote is rejected. The cluster is stuck with no primary.
    # See MDB-41951: plan_primary_shut() race — rewinds to lock_holder before the
    # candidate reaches PROMOTED phase.
    (r'Promote is not allowed with given configuration',
     'Failover blocked: promote not safe after failed switchover (old primary rewound to ex-candidate before promote)', 91),
    # Old primary rewound to the candidate (lock_holder) during primary_shut
    # phase — before the candidate promoted. This is the race condition root
    # cause: plan_primary_shut() checks lock_holder != None but not the phase.
    (r'SWITCHOVER: new primary found, returning to cluster',
     'Old primary rewound to candidate during primary_shut (race: candidate had lock but not yet promoted)', 93),
    # Switchover state desync: ZK switchover primary != actual primary.
    # After a failed promote the old primary restarts as a replica streaming from
    # the ex-candidate; db_state['primary_fqdn'] then differs from switchover.hostname.
    # This causes _check_replica_switchover() to return False, bypassing is_failed()
    # guard — see MDB-41951 Bug #8 (Fix #8 added an early guard in replica_iter).
    (r'current primary FQDN is not equal to hostname in switchover node',
     'Switchover state desync: ZK switchover primary != actual primary (stale switchover record)', 70),
    # Fix #8 applied: replica_iter early FQDN-mismatch guard triggered.
    # The replica detected FAILED phase + no lock holder before _check_replica_switchover
    # could reject it due to FQDN mismatch, and fell back to failover.
    (r'falling back to failover \(early FQDN-mismatch guard, MDB-41951\)',
     'replica_iter FQDN-mismatch early guard triggered (Fix #8 applied — cluster recovering primary)', 5),
    # Bug #9 symptom: failover blocked by libpq check on ex-candidate that is alive as replica.
    # is_host_unreachable(check_primary=False) connects without target_session_attrs=primary,
    # so a live replica is seen as "reachable" → "primary still accessible" → failover aborted.
    # Fix #9: skip this check when switchover_in_progress=True.
    (r'primary has died but it is still accessible through libpq\. Not doing anything',
     'Failover blocked: libpq check found ex-candidate alive as replica (switchover_in_progress bypass missing)', 88),
    # Coordinator stuck in DETECTED phase: primary recovered and is_primary_unreachable=False,
    # so _gates_pass() returns False → coordinator returns [] → no phase transition → infinite loop.
    # Root cause: DETECTED phase re-checked gates on every iteration, including is_primary_unreachable.
    # Fix: split DETECTED into DETECTED (gate check once) + WALRECEIVER_DISABLING (unconditional ops).
    # failover_with_network_inconsistency.feature:157 — replicas never vote.
    (r'Primary still accessible through libpq, not doing failover',
     'Coordinator stuck in DETECTED phase: primary recovered, gates fail → no phase transition (election_vote keys never appear)', 92),
    (r'ACTION-FAILED\. Could not simple switch to primary',
     'Simple primary switch failed (WAL likely diverged)', 90),
    (r'Could not do a simple primary switch.*Simple primary switch tried: True',
     'Simple primary switch exhausted, should proceed to pg_rewind', 85),
    # Simple switch succeeded when the node was formerly primary — pg_rewind
    # was skipped. ReturnObservation.build() reads role=None from dead PG
    # state, so _derive_phase() picks SIMPLE_SWITCH instead of REWIND.
    # The test then fails on "was rewinded" assertion (no /tmp/rewind_called).
    (r'Simple switch primary to .* succeeded',
     'Simple primary switch succeeded — pg_rewind skipped (check if node was primary: ReturnObservation may have read role=None from dead PG)', 89),
    # Entry point for return-to-cluster — useful for correlating which phase
    # the state machine chose (SIMPLE_SWITCH vs REWIND).
    (r'Starting return to cluster\. New primary:',
     'Return-to-cluster started (correlate with next phase: simple switch or rewind)', 10),
    # Side-replica disabled archive restore (restore_command=/bin/false) before
    # switching to the switchover candidate. If the candidate is unreachable
    # (e.g. network disconnected), the replica cannot start archive recovery
    # and falls back to pg_rewind — even though timelines have not diverged.
    # This is a key signal for "unnecessary rewind" root-cause analysis.
    (r'Setting restore_command to /bin/false',
     'Side-replica disabled archive restore (restore_command=/bin/false) before switchover switch', 82),
    # pg_rewind was invoked but reported "no rewind required" — the source and
    # target are on the same timeline. This means the rewind was unnecessary:
    # the simple primary switch failed for a transient reason (archive recovery
    # timeout, candidate unreachable) rather than actual WAL divergence.
    (r'pg_rewind: no rewind required|source and target cluster are on the same timeline',
     'Unnecessary pg_rewind: source and target on same timeline (simple switch failed for transient reason, not WAL divergence)', 86),
    # Simple primary switch failed specifically because archive recovery check
    # timed out — the replica could not start streaming from the new primary
    # within the recovery timeout. Often caused by restore_command=/bin/false
    # blocking archive recovery while the new primary is unreachable.
    (r'Simple primary switch: archive recovery check failed, falling back to rewind',
     'Simple primary switch failed: archive recovery check timed out (restore_command=/bin/false or new primary unreachable)', 88),
    (r'Error while using pg_rewind',
     'pg_rewind failed', 80),
    (r'rewind_fail\.flag|Could not rewind.*times, setting rewind-failed flag',
     'Rewind failed flag set (max_rewind_retries exceeded)', 75),
    (r'FAILOVER: Primary has died',
     'Failover triggered (primary unavailable)', 50),
    # Participant cannot vote: host_lsn is None because lwaldump() crashed
    # after walreceiver was disabled. Root cause: state machine disables
    # walreceiver before voting (registration phase runs after WALRECEIVER_DISABLING).
    # Fix: vote (registration) must happen before walreceiver disable (MDB-41951).
    (r'Cannot vote: host_lsn unavailable',
     'Participant cannot vote: host_lsn is None (lwaldump() crashed after walreceiver disable — vote must happen before disable)', 93),
    # Coordinator waiting for all alive hosts to vote but a participant is stuck
    # (host_lsn unavailable). Combined with "Cannot vote" pattern above, this
    # pinpoints the vote-before-disable phase ordering bug.
    (r'Waiting for all alive hosts to vote',
     'Coordinator stuck waiting for votes (participant cannot vote — check for "Cannot vote: host_lsn unavailable" on other hosts)', 90),
    # Failover winner_selected but the winner never acquires the primary lock.
    # Root cause: winner == coordinator → _run_failover_step routes to the
    # coordinator machine whose plan_winner_selected only waits (empty Plan).
    # The winner never runs the participant AcquireLock → failover stalls.
    (r'Failover state: winner_selected',
     'Failover stuck in winner_selected (winner never acquires primary lock — winner-is-coordinator deadlock)', 95),
    # Failover stuck in promoting: the winner acquired the leader lock and ZK
    # failover_state transitioned to 'promoting', but replica_iter sees
    # holder == my_hostname and skips the _run_failover_step call (holder is
    # not None). The participant machine (DoFailover → promote) never runs.
    # Root cause: replica_iter missing active-failover guard when holder==self.
    (r'Failover state: promoting',
     'Failover stuck in promoting (winner holds lock but replica_iter never calls _run_failover_step — promote never runs)', 95),
    # dead_iter released the leader lock during an active switchover — the old
    # primary stopped PG (pg_stopped phase) and on the next iteration PG is dead,
    # so run_iteration dispatches to dead_iter which unconditionally calls
    # release_if_hold(PRIMARY_LOCK_PATH). This prematurely hands the lock to the
    # candidate before the switchover machine reaches primary_shut, causing the
    # old primary to start PG again and race with the candidate's promote.
    (r'Seems that all hosts \(including me\) are dead\. Trying to start PostgreSQL',
     'dead_iter prematurely released leader lock during switchover (old primary restarted PG before primary_shut)', 94),
    # dead_iter switchover guard loop: the guard prevents lock release but never
    # calls PrimarySwitchoverMachine.plan(), so the old primary gets stuck in
    # an infinite loop (PG dead → dead_iter → guard → return None → repeat).
    # The switchover never advances from pg_stopped to primary_shut.
    (r'Switchover in progress.*local PG is dead.*waiting for switchover state machine',
     'dead_iter stuck in switchover guard loop (state machine never called to advance pg_stopped → primary_shut)', 90),
    # dead_iter calls the switchover state machine, but the observation builder
    # (_build_switchover_observation / SwitchoverObservation.build) makes
    # PG-dependent reads (db.get_replics_info, db.get_role) that raise
    # PostgresConnectionError when PG is dead. The exception propagates to
    # run_iteration and restarts the iteration — the state machine never
    # advances, trapping the old primary in an infinite loop.
    (r'Skipping PG-dependent reads in switchover observation',
     'dead_iter observation builder skips PG reads (fix applied — state machine advances despite dead PG)', 5),
    # Replica stuck in switchover FAILED phase (FIXED in MDB-41951): replica_iter
    # now falls back to failover when switchover phase is FAILED and there is no
    # lock holder. If this pattern still appears, the fix has not been deployed.
    (r'Switchover in progress \(phase failed\), waiting',
     'replica stuck in switchover FAILED phase — fix not deployed (replica_iter missing failover fallback)', 92),
    # Fix applied: replica_iter detected FAILED phase with no lock holder and
    # triggered failover fallback so the cluster can recover a primary.
    (r'Switchover failed \(phase failed\) and no primary lock holder — falling back to failover',
     'replica_iter FAILED-phase failover fallback triggered (fix applied — cluster recovering primary)', 5),
    (r'Participate in election|Successfully voted',
     'Election participation', 30),
    (r'Sleep for test purposes for an election loser',
     'Election loser sleep (test debug delay)', 25),
    (r'Seems that primary has been switched to.*We should switch primary',
     'Primary switch detected by replica', 40),
    (r'Retrying timeout expired\.',
     'Retry timeout expired (operation did not complete in time)', 20),
    (r'PostgreSQL is dead',
     'PostgreSQL reported dead', 60),
    (r'Could not connect to',
     'PostgreSQL connection failure', 55),
    (r'connection to server at.*failed',
     'Network connection failure to PostgreSQL host', 45),
    (r'could not restore file.*from archive',
     'Archive restore failure (WAL segment or history file missing)', 35),
    (r'record with incorrect prev-link',
     'WAL corruption: incorrect prev-link', 70),
    (r'unexpected pageaddr.*in log segment',
     'WAL page address mismatch', 65),
    (r'HA replica shouldn\'t exist inside a single node cluster',
     'HA replica in single-node cluster', 15),
    (r'ZK.*session.*expired|Zookeeper.*session.*expired',
     'ZooKeeper session expired', 50),
    # Python runtime crash in pgconsul itself — an unhandled exception
    # propagates to run_iteration and restarts the loop every second,
    # preventing the node from recovering. The traceback line in
    # pgconsul.log is the strongest signal of a code bug (not an
    # infrastructure issue). Attribute/Type/Key errors on db_state are
    # classic type-contract violations between layers.
    (r'AttributeError: .* object has no attribute',
     'pgconsul code crash: AttributeError in run_iteration (type contract violation — check db_state type passed to ReturnObservation.build)', 97),
    (r'TypeError: .* argument',
     'pgconsul code crash: TypeError in run_iteration (argument type mismatch)', 96),
    (r'KeyError: ',
     'pgconsul code crash: KeyError in run_iteration (missing dict key)', 95),
    # Python import failures — pgconsul crashes on startup when a module is
    # missing from the installed package (e.g. setup.py did not include a
    # subpackage). This is a fatal startup error, not a runtime condition.
    (r'ModuleNotFoundError: No module named [\'"]pgconsul\.',
     'pgconsul startup crash: missing subpackage in installed package (check setup.py packages=)', 99),
    (r'ImportError: cannot import name .* from [\'"]pgconsul',
     'pgconsul startup crash: import failure from pgconsul package (check setup.py packages=)', 98),
]

_POSTGRES_RAW: list[tuple[str, str, int]] = [
    (r'requested starting point .* is ahead of the WAL flush position',
     'WAL divergence: requested start point ahead of flush position', 100),
    (r'FATAL:.*could not receive data from WAL stream',
     'WAL stream FATAL error', 90),
    (r'FATAL:.*terminating walreceiver process',
     'Walreceiver terminated', 60),
    (r'record with incorrect prev-link',
     'WAL record prev-link mismatch (timeline divergence)', 85),
    (r'unexpected pageaddr.*in log segment',
     'Unexpected WAL page address', 80),
    (r'could not restore file.*from archive',
     'Archive file not found (exit code 23)', 50),
    (r'new target timeline is \d+',
     'Timeline switch detected', 30),
    (r'started streaming WAL from primary',
     'Streaming started (may have failed immediately after)', 20),
    (r'FATAL:.*requested.*has already been removed',
     'Replication slot removed', 55),
    (r'ERROR:.*replication slot.*does not exist',
     'Replication slot missing', 50),
]

_ZOOKEEPER_RAW: list[tuple[str, str, int]] = [
    (r'SessionExpired|session expired',
     'ZooKeeper session expired', 60),
    (r'ConnectionLoss|connection loss',
     'ZooKeeper connection loss', 50),
]

# Patterns that indicate a stuck/looping state (not a single error, but
# repeated behavior that leads to timeout).
_STUCK_RAW: list[tuple[str, str]] = [
    (r'Waiting \d+\.\d+ for PostgreSQL started streaming from',
     'Repeatedly waiting for streaming (stuck in streaming wait loop)'),
    (r'Waiting \d+\.\d+ for PostgreSQL started archive recovery',
     'Repeatedly waiting for archive recovery (stuck in recovery loop)'),
    (r'Waiting \d+\.\d+ for PostgreSQL has completed recovery',
     'Repeatedly waiting for recovery completion'),
    (r'could not restore file.*from archive',
     'Repeatedly failing to restore from archive'),
    (r'Retrying timeout expired',
     'Repeated retry timeouts'),
    (r'primary_switch checks is \d+',
     'Primary switch attempt counter (check if it grows slowly)'),
    # Switchover-specific stuck patterns
    (r'Switchover in progress, waiting for candidate to be chosen.*state: failed',
     'Switchover stuck in failed state (replicas waiting, no cleanup)'),
    (r'No lock instance for switchover/lock\. Creating one\.',
     'Switchover lock repeatedly not found (primary stuck in post-switchover loop)'),
    # Candidate repeatedly failing to promote — no transition to FAILED state
    (r'ACTION\. Starting promote',
     'Promote retry loop (candidate repeatedly failing to promote)'),
    # Candidate has no handler for a switchover phase — repeated, indicates stuck
    (r'No candidate-side planner for switchover phase',
     'Candidate stuck: no planner for switchover phase (repeated)'),
    # Switchover state desync — repeated, stale switchover record in ZK
    (r'current primary FQDN is not equal to hostname in switchover node',
     'Switchover state desync (repeated: ZK switchover primary != actual primary)'),
    # Old primary stuck after switchover failed: it restarted PG in dead_iter,
    # became replica of the candidate (which also failed to promote), and now
    # loops on "current primary FQDN != switchover node hostname" forever.
    (r'Lock in ZK is being held by.*We should return to cluster here',
     'Old primary returning to cluster mid-switchover (lock stolen by candidate before primary_shut)'),
    # Candidate reached candidate_acquired/promoted but never logged
    # "SWITCHOVER STARTED" — the entry-point event was lost during ADR-0006
    # migration. Behave tests asserting "SWITCHOVER STARTED" in order will fail.
    (r'SWITCHOVER PHASE → candidate_acquired',
     'Candidate promoted without SWITCHOVER STARTED event (entry-point log lost in ADR-0006 migration)'),
    # Failover loop: replica repeatedly enters failover but never promotes.
    # Combined with "Failover state: winner_selected" error pattern above,
    # this pinpoints the winner-is-coordinator deadlock.
    (r'FAILOVER: Primary has died, starting failover procedure',
     'Failover retry loop (replica repeatedly entering failover without promoting)'),
    # Failover stuck in promoting: winner holds the lock, ZK state is
    # 'promoting', but replica_iter never calls _run_failover_step because
    # holder == my_hostname (not None). DoFailover never runs → no promote.
    (r'Failover state: promoting',
     'Failover stuck in promoting (winner holds lock but DoFailover never runs — replica_iter missing active-failover guard)'),
    # Participant stuck: cannot vote because host_lsn is None (lwaldump crashed
    # after walreceiver disable). Coordinator waits for votes forever.
    (r'Cannot vote: host_lsn unavailable',
     'Participant stuck: cannot vote (host_lsn is None — lwaldump crashed after walreceiver disable)'),
]


# Compiled, immutable pattern sets.
PGCONSUL_PATTERNS: tuple[Pattern, ...] = tuple(
    Pattern.from_tuple(p) for p in _PGCONSUL_RAW
)
POSTGRES_PATTERNS: tuple[Pattern, ...] = tuple(
    Pattern.from_tuple(p) for p in _POSTGRES_RAW
)
ZOOKEEPER_PATTERNS: tuple[Pattern, ...] = tuple(
    Pattern.from_tuple(p) for p in _ZOOKEEPER_RAW
)
STUCK_PATTERNS: tuple[StuckPattern, ...] = tuple(
    StuckPattern.from_tuple(p) for p in _STUCK_RAW
)

# Map a log_type to its error-scan pattern set.
PATTERNS_BY_LOG_TYPE: dict[str, tuple[Pattern, ...]] = {
    "pgconsul": PGCONSUL_PATTERNS,
    "postgresql": POSTGRES_PATTERNS,
    "zookeeper": ZOOKEEPER_PATTERNS,
}


def all_error_patterns() -> tuple[Pattern, ...]:
    """Return every error pattern across all log types, in declaration order."""
    return PGCONSUL_PATTERNS + POSTGRES_PATTERNS + ZOOKEEPER_PATTERNS


def weight_of(pattern: Pattern) -> int:
    """Return the weight of a pattern (helper for sorting)."""
    return pattern.weight


def patterns_for(log_type: str) -> tuple[Pattern, ...]:
    """Return the error patterns registered for *log_type* (empty if unknown)."""
    return PATTERNS_BY_LOG_TYPE.get(log_type, ())


def combined_stuck_regex() -> str:
    """Return a single alternation regex matching any stuck pattern.

    Used by the grep pre-filter for large postgresql logs so the whole file is
    scanned in one pass instead of line-by-line in Python.
    """
    return "|".join(f"({p.regex})" for p in STUCK_PATTERNS)


def _unique_names(patterns: Iterable) -> bool:
    seen: set[str] = set()
    for p in patterns:
        if p.name in seen:
            return False
        seen.add(p.name)
    return True


# Names only need to be unique *within* a single pattern set: the scanner
# reports the first matching pattern per line, and dedup keys on
# (container, pattern_name) within one scan. The same name across different
# sets (e.g. "ZooKeeper session expired" in both pgconsul and zk) is fine,
# because Pattern now carries its own weight — no global name->weight map.
assert _unique_names(PGCONSUL_PATTERNS), "duplicate pgconsul pattern names"
assert _unique_names(POSTGRES_PATTERNS), "duplicate postgres pattern names"
assert _unique_names(ZOOKEEPER_PATTERNS), "duplicate zookeeper pattern names"
assert _unique_names(STUCK_PATTERNS), "duplicate stuck pattern names"
