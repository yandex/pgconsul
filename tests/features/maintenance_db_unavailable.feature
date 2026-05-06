Feature: Primary temporary unavailable in maintenance mode should not stop pooler

    # Regression test for MDB-43333.
    #
    # Root cause: in update_maintenance_status() the role was obtained BEFORE
    # get_state() was called. If PostgreSQL became unavailable between those two
    # calls, role='primary' but db_state={'alive': False, 'timeline': None}.
    # The old code treated db_timeline=None as a failover indicator and stopped
    # odyssey + reset archive_command, even though the cluster was in maintenance.
    #
    # Fix: destructive operations (pgpooler stop / stop_archiving_wal) are now
    # guarded by db_state['alive'] == True.

    @mdb_43333
    Scenario: Primary postgres temporarily unavailable in maintenance should not stop pooler
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
                    sync_replication_in_maintenance: 'no'
                replica:
                    allow_potential_data_loss: 'no'
                    primary_switch_checks: 1
                    min_failover_timeout: 1
                    primary_unavailability_timeout: 2
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "zookeeper" with replication slots
        """
            postgresql1:
                role: primary
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
        And container "postgresql2" is in quorum group
        And container "postgresql3" is in quorum group
        And container "postgresql2" is a replica of container "postgresql1"
        And container "postgresql3" is a replica of container "postgresql1"
        Then "pgbouncer" is running in container "postgresql1"
        When we set value "enable" for key "/pgconsul/postgresql/maintenance" in zookeeper "zookeeper1"
        And we wait "10.0" seconds
        Then "pgbouncer" is running in container "postgresql1"
        # Simulate temporary PostgreSQL unavailability on the primary while in maintenance.
        # pgconsul will get role='primary' from the previous iteration, then fail to
        # connect to PostgreSQL (db_state={'alive': False, 'timeline': None}).
        # With the MDB-43333 fix, this must NOT stop odyssey or reset archive_command.
        When we gracefully stop "postgres" in container "postgresql1"
        And we wait "5.0" seconds
        Then "pgbouncer" is running in container "postgresql1"
        # Restore PostgreSQL and verify the cluster is still healthy
        When we start "postgres" in container "postgresql1"
        And we wait "10.0" seconds
        Then "pgbouncer" is running in container "postgresql1"
        And zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        When we set value "disable" for key "/pgconsul/postgresql/maintenance" in zookeeper "zookeeper1"
        And we wait "10.0" seconds
        Then zookeeper "zookeeper1" has value "None" for key "/pgconsul/postgresql/maintenance"
        And container "postgresql1" became a primary
        And container "postgresql2" is a replica of container "postgresql1"
        And container "postgresql3" is a replica of container "postgresql1"
        And "pgbouncer" is running in container "postgresql1"
