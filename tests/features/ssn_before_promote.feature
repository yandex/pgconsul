Feature: SSN is set before promote to prevent data-loss window

    # ---------------------------------------------------------------------------
    # Scenario 1: Failover — SSN is set immediately before promote
    #
    # Verifies that synchronous_standby_names is not empty right after the new
    # primary appears. If SSN were set only on the next pgconsul iteration, it
    # would be empty (async) in the window immediately after pg_ctl promote.
    # ---------------------------------------------------------------------------
    @failover
    Scenario: SSN is set before promote during failover (quorum mode)
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: 'yes'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 1
                    min_failover_timeout: 1
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
                            priority: 2
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 1
        """
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in quorum group
        Then container "postgresql3" is in quorum group

        # Kill the primary to trigger failover
        When we disconnect from network container "postgresql1"

        # New primary should appear (postgresql2 has the highest priority)
        Then container "postgresql2" became a primary
        Then container "postgresql2" pgconsul log contains messages in order within "60" seconds
        """
        ACTION. Setting SSN before promote
        ACTION. Setting synchronous_standby_names to ANY 1(pgconsul_postgresql1_1_pgconsul_pgconsul_net,pgconsul_postgresql3_1_pgconsul_pgconsul_net)
        Set SSN before promote
        ACTION. Starting promote
        """
        Then postgresql in container "postgresql2" has option "synchronous_standby_names"
        """
        ANY 1(pgconsul_postgresql1_1_pgconsul_pgconsul_net,pgconsul_postgresql3_1_pgconsul_pgconsul_net)
        """

        When we connect to network container "postgresql1"
        # Cluster recovered correctly
        Then container "postgresql1" is a replica of container "postgresql2"
        And container "postgresql3" is a replica of container "postgresql2"

    # ---------------------------------------------------------------------------
    # Scenario 2: Switchover — SSN is set before the candidate is promoted
    #
    # Verifies that the switchover candidate sets SSN (including the old primary
    # as a future replica) before calling pg_ctl promote. This prevents the
    # data-loss window between promote and the first regular pgconsul iteration.
    # ---------------------------------------------------------------------------
    @switchover_test
    Scenario: SSN is set before promote during switchover (quorum mode)
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: 'yes'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 1
                    min_failover_timeout: 120
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
                            priority: 2
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 1
        """
        Then container "postgresql3" is in quorum group
        And container "postgresql2" is in quorum group

        # Initiate switchover from postgresql1 to postgresql2 (highest priority)
        When we lock "/pgconsul/postgresql/switchover/lock" in zookeeper "zookeeper1"
        And we set value "{'hostname': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net','timeline': 1}" for key "/pgconsul/postgresql/switchover/master" in zookeeper "zookeeper1"
        And we set value "scheduled" for key "/pgconsul/postgresql/switchover/state" in zookeeper "zookeeper1"
        And we release lock "/pgconsul/postgresql/switchover/lock" in zookeeper "zookeeper1"

        # New primary should appear (postgresql2 has the highest priority)
        Then container "postgresql2" became a primary
        Then container "postgresql2" pgconsul log contains messages in order within "60" seconds
        """
        ACTION. Setting SSN before promote
        ACTION. Setting synchronous_standby_names to ANY 1(pgconsul_postgresql1_1_pgconsul_pgconsul_net,pgconsul_postgresql3_1_pgconsul_pgconsul_net)
        Set SSN before promote
        ACTION. Starting promote
        """
        Then postgresql in container "postgresql2" has option "synchronous_standby_names"
        """
        ANY 1(pgconsul_postgresql1_1_pgconsul_pgconsul_net,pgconsul_postgresql3_1_pgconsul_pgconsul_net)
        """

        # Cluster recovered correctly
        And container "postgresql1" is a replica of container "postgresql2"
        And container "postgresql3" is a replica of container "postgresql2"
