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
        Then container "postgresql1" is a replica of container "postgresql2" and streaming
        Then container "postgresql3" is a replica of container "postgresql2" and streaming

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
        Then container "postgresql1" is a replica of container "postgresql2" and streaming
        Then container "postgresql3" is a replica of container "postgresql2" and streaming

    # ---------------------------------------------------------------------------
    # Scenario 3: Failover after postgresql3 was evicted from quorum
    #
    # postgresql3 is disconnected and removed from QUORUM_PATH. Then
    # postgresql1 fails, postgresql2 promotes, and pgconsul rewrites SSN
    # from dead postgresql1/postgresql3 to async / empty.
    # ---------------------------------------------------------------------------
    @failover_with_dead_ha_replica
    Scenario: SSN before promote with long-dead HA replicas
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: 'yes'
                primary:
                    before_async_unavailability_timeout: 0
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                    quorum_removal_delay: 10
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

        # Disconnect postgresql3 and wait until it is evicted from QUORUM_PATH.
        When we disconnect from network container "postgresql3"
        And we wait "30.0" seconds
        Then zookeeper "zookeeper1" has value "['pgconsul_postgresql2_1.pgconsul_pgconsul_net']" for key "/pgconsul/postgresql/quorum"

        When we disconnect from network container "postgresql1"

        # postgresql2 promotes with SSN that still contains dead postgresql1/postgresql3.
        Then container "postgresql2" became a primary
        Then postgresql in container "postgresql2" has option "synchronous_standby_names"
        """
        ANY 1(pgconsul_postgresql1_1_pgconsul_pgconsul_net,pgconsul_postgresql3_1_pgconsul_pgconsul_net)
        """

        # Then pgconsul rewrites SSN to async / empty.
        Then postgresql in container "postgresql2" has empty option "synchronous_standby_names"

        # Cluster recovers when disconnected hosts come back.
        When we connect to network container "postgresql1"
        And we connect to network container "postgresql3"
        Then container "postgresql1" is a replica of container "postgresql2" and streaming
        Then container "postgresql3" is a replica of container "postgresql2" and streaming
