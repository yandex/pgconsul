Feature: Switchover survives pgconsul kill -9 in mid-phases

    ADR-0005 §3: switchover state machine persists phase to ZK before each
    action, so a kill -9 (SIGKILL) of pgconsul at any phase must not corrupt
    the switchover.  After pgconsul is restarted manually, it reads the
    persisted phase from ZK and resumes the state machine.

    In the test environment supervisord has autorestart=false for pgconsul,
    so manual restart is required after kill -9.

    @switchover
    Scenario: Kill -9 pgconsul on primary in turning-sides phase
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: 'yes'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 3
                replica:
                    allow_potential_data_loss: 'no'
                    primary_switch_checks: 3
                    min_failover_timeout: 120
                    primary_unavailability_timeout: 10
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "zookeeper" with replication slots
        """
            postgresql1:
                role: primary
                config:
                    pgconsul.conf:
                        global:
                            priority: 3
            postgresql2:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 1
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 2
        """
        Then container "postgresql3" is in quorum group
        When we do targeted switchover from container "postgresql1" to container "postgresql2"
        # The candidate prepares slots and side replicas are turning to it.
        Then zookeeper "zookeeper1" has switchover phase "turning_sides"
        # Kill -9 pgconsul on primary; supervisord will NOT auto-restart (autorestart=false in test env)
        When we kill "pgconsul" in container "postgresql1" with signal "SIGKILL"
        # Allow ZK session to expire so the leader lock is released (~10s at iteration_timeout=1)
        And we wait "15.0" seconds
        # Restart pgconsul on the old primary — it resumes the manager-owned protocol.
        And we start "pgconsul" in container "postgresql1"
        # Switchover must resume and complete after restart
        Then container "postgresql2" became a primary
        And container "postgresql3" is a replica of container "postgresql2"
        And container "postgresql1" is a replica of container "postgresql2"
        And container "postgresql1" is in quorum group
        And postgresql in container "postgresql3" was not rewinded
        And postgresql in container "postgresql1" was rewinded
        And timing log in container "postgresql2" contains "switchover,downtime"

    @switchover
    Scenario: Kill -9 pgconsul on primary in preparing-bridge phase
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: 'yes'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 3
                replica:
                    allow_potential_data_loss: 'no'
                    primary_switch_checks: 3
                    min_failover_timeout: 120
                    primary_unavailability_timeout: 10
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "zookeeper" with replication slots
        """
            postgresql1:
                role: primary
                config:
                    pgconsul.conf:
                        global:
                            priority: 3
            postgresql2:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 1
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 2
        """
        Then container "postgresql3" is in quorum group
        When we do targeted switchover from container "postgresql1" to container "postgresql2"
        # Candidate has turned the required side replica and prepares bridge SSN.
        Then zookeeper "zookeeper1" has switchover phase "preparing_bridge"
        # Kill -9 pgconsul on primary; supervisord will NOT auto-restart (autorestart=false in test env)
        When we kill "pgconsul" in container "postgresql1" with signal "SIGKILL"
        # Allow ZK session to expire so the leader lock is released; candidate can take it and promote
        And we wait "15.0" seconds
        # Restart pgconsul on the old primary — it resumes the manager-owned protocol.
        And we start "pgconsul" in container "postgresql1"
        # Switchover must complete — candidate took over during the kill window
        Then container "postgresql2" became a primary
        And container "postgresql3" is a replica of container "postgresql2"
        And container "postgresql1" is a replica of container "postgresql2"
        And container "postgresql1" is in quorum group
        And postgresql in container "postgresql3" was not rewinded
        And postgresql in container "postgresql1" was rewinded
        And timing log in container "postgresql2" contains "switchover,downtime"
