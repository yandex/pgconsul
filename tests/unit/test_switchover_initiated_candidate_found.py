"""Red unit test: plan_initiated must not waste an iteration on candidate_found.

Reproduces pgconsul_util.feature:402 — targeted switchover times out because
the primary spends an extra iteration (~4s in Docker tests) between detecting
``live_switchover_state == CANDIDATE_FOUND`` and actually stopping the pooler.

Bug: ``plan_initiated`` returns only prep commands (Log, StoreReplicsInfo,
Checkpoint) when it detects CANDIDATE_FOUND. The pooler is stopped only on the
*next* iteration in ``plan_candidate_found``. This wastes one full iteration
(~4s) and pushes the total switchover duration past the 60s ``--block`` timeout.

Fix: ``plan_initiated`` should inline the pooler-stop + transition when
CANDIDATE_FOUND is detected, eliminating the extra iteration.
"""

from src.commands import (
    Checkpoint,
    Log,
    StartTimer,
    StopPooler,
    StoreReplicsInfo,
    TransitionTo,
    WriteLocalState,
)
from src.switchover import (
    PrimarySwitchoverMachine,
    SwitchoverMachineConfig,
    SwitchoverObservation,
    SwitchoverPhase,
    SwitchoverRecord,
)


def _make_record(phase, candidate='host2', destination='host2'):
    return SwitchoverRecord(
        hostname='host1',
        timeline=5,
        destination=destination,
        phase=phase,
        candidate=candidate,
        side_replicas=['host3'],
    )


def _make_obs(
    phase,
    *,
    candidate='host2',
    downtime_timer_started=False,
    live_switchover_state=None,
    my_hostname='host1',
):
    """Build a minimal SwitchoverObservation for plan_initiated tests."""
    return SwitchoverObservation(
        record=_make_record(phase, candidate=candidate),
        my_hostname=my_hostname,
        role='primary',
        zk_timeline=5,
        failover_state=None,
        last_failover_ts=None,
        last_switchover_ts=None,
        ha_replics=frozenset({'host2', 'host3'}),
        replics_info=[{'application_name': 'host2', 'state': 'streaming', 'replay_lag_msec': 0}],
        streaming_replicas=('host2', 'host3'),
        live_switchover_state=live_switchover_state,
        candidate_alive=True,
        lock_holder=None,
        switchover_timer_started=False,
        downtime_timer_started=downtime_timer_started,
        downtime_started_ts=None,
        candidate=candidate,
        side_replicas=('host3',),
        all_side_replicas_turned=False,
        switchover_primary_info=None,
        switchover_candidate=None,
    )


def _make_machine():
    return PrimarySwitchoverMachine(None, config=SwitchoverMachineConfig())


class TestPlanInitiatedCandidateFoundDetected:
    """plan_initiated must inline pooler stop when CANDIDATE_FOUND is detected.

    Before the fix, plan_initiated returned only prep commands (Log +
    StoreReplicsInfo + Checkpoint), deferring StopPooler to the next iteration
    via plan_candidate_found. This wasted one full iteration (~4s in Docker
    tests), pushing the total switchover duration past the 60s --block timeout.
    """

    def test_includes_stop_pooler_when_candidate_found_detected(self):
        """plan_initiated must include StopPooler when CANDIDATE_FOUND detected.

        Reproduces pgconsul_util.feature:402.
        """
        m = _make_machine()
        obs = _make_obs(
            SwitchoverPhase.INITIATED,
            live_switchover_state=SwitchoverPhase.CANDIDATE_FOUND,
        )
        plan = m.plan_initiated(obs)
        assert StopPooler() in plan, (
            'plan_initiated must stop the pooler in the same iteration when '
            'CANDIDATE_FOUND is detected — deferring to plan_candidate_found '
            'wastes ~4s and causes --block timeout (pgconsul_util.feature:402)'
        )

    def test_includes_local_transition_to_pooler_stopped(self):
        m = _make_machine()
        obs = _make_obs(
            SwitchoverPhase.INITIATED,
            live_switchover_state=SwitchoverPhase.CANDIDATE_FOUND,
        )
        plan = m.plan_initiated(obs)
        assert WriteLocalState('switchover_primary', SwitchoverPhase.POOLER_STOPPED) in plan, (
            'plan_initiated must persist POOLER_STOPPED locally in the same '
            'iteration when CANDIDATE_FOUND is detected'
        )

    def test_prep_commands_precede_pooler_stop(self):
        """StoreReplicsInfo and Checkpoint must come before StopPooler."""
        m = _make_machine()
        obs = _make_obs(
            SwitchoverPhase.INITIATED,
            live_switchover_state=SwitchoverPhase.CANDIDATE_FOUND,
        )
        plan = m.plan_initiated(obs)
        store_idx = next(
            (i for i, c in enumerate(plan) if isinstance(c, StoreReplicsInfo)),
            None,
        )
        checkpoint_idx = next(
            (i for i, c in enumerate(plan) if isinstance(c, Checkpoint)),
            None,
        )
        pooler_idx = next(
            (i for i, c in enumerate(plan) if isinstance(c, StopPooler)),
            None,
        )
        assert store_idx is not None, 'StoreReplicsInfo must be in the plan'
        assert checkpoint_idx is not None, 'Checkpoint must be in the plan'
        assert pooler_idx is not None, 'StopPooler must be in the plan'
        assert store_idx < pooler_idx, 'StoreReplicsInfo must precede StopPooler'
        assert checkpoint_idx < pooler_idx, 'Checkpoint must precede StopPooler'

    def test_starts_downtime_timer(self):
        """plan_initiated must start the downtime timer when CANDIDATE_FOUND detected."""
        m = _make_machine()
        obs = _make_obs(
            SwitchoverPhase.INITIATED,
            live_switchover_state=SwitchoverPhase.CANDIDATE_FOUND,
            downtime_timer_started=False,
        )
        plan = m.plan_initiated(obs)
        assert StartTimer('downtime') in plan

    def test_skips_downtime_timer_if_already_started(self):
        """plan_initiated must not start downtime timer if already running."""
        m = _make_machine()
        obs = _make_obs(
            SwitchoverPhase.INITIATED,
            live_switchover_state=SwitchoverPhase.CANDIDATE_FOUND,
            downtime_timer_started=True,
        )
        plan = m.plan_initiated(obs)
        assert StartTimer('downtime') not in plan

    def test_emits_candidate_found_detected_log(self):
        """plan_initiated must still emit the detection log event."""
        m = _make_machine()
        obs = _make_obs(
            SwitchoverPhase.INITIATED,
            live_switchover_state=SwitchoverPhase.CANDIDATE_FOUND,
        )
        plan = m.plan_initiated(obs)
        log_cmds = [c for c in plan if isinstance(c, Log)]
        assert any('candidate_found detected' in c.message for c in log_cmds)

    def test_returns_empty_when_still_waiting(self):
        """plan_initiated returns empty plan when CANDIDATE_FOUND not yet detected."""
        m = _make_machine()
        obs = _make_obs(
            SwitchoverPhase.INITIATED,
            live_switchover_state=SwitchoverPhase.INITIATED,
        )
        plan = m.plan_initiated(obs)
        assert plan == []

    def test_aborts_when_candidate_is_none(self):
        """plan_initiated aborts when candidate is None."""
        m = _make_machine()
        obs = _make_obs(
            SwitchoverPhase.INITIATED,
            candidate=None,
            live_switchover_state=SwitchoverPhase.CANDIDATE_FOUND,
        )
        plan = m.plan_initiated(obs)
        assert plan == [TransitionTo(SwitchoverPhase.FAILED)]
