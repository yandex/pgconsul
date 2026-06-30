Feature: Overloaded postgres (primary and replica) is not restarted by pgconsul

    # Regression test for MDB-46149.
    #
    # Bug: connection timeout was treated as "postgres dead" → dead_iter() → restart.
    # Fix: run_iteration() catches PGConnectionTimeout, records first failure timestamp
    # (_pg_first_failure_ts in main.py, grace period = pg_conn_failure_grace_period seconds).
    # While elapsed < grace period and process alive → "Skipping". At grace period → "Forcing action.".
    # pg.py uses _conn_timeout_count for exponential connect_timeout backoff (1→2→4→8→10 s).

    @skipping_restart
    Scenario: Overloaded primary is not restarted while systemctl reports it running and timeout counter is reset after successful reconnection
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: 'yes'
                    pg_conn_failure_grace_period: 30
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                replica:
                    allow_potential_data_loss: 'no'
                    primary_switch_checks: 1
                    min_failover_timeout: 120
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
        And "pgbouncer" is running in container "postgresql1"

        When we remember postgresql start time in container "postgresql1"

        # --- Phase 1: first overload within grace period (pg_conn_failure_grace_period=30s) ---
        # SIGSTOP simulates overload: process alive (systemctl=running) but connection times out.
        # 5s elapsed < 30s grace period → Skipping.
        When we kill "postgres" in container "postgresql1" with signal "STOP"
        When we wait "5.0" seconds
        When we kill "postgres" in container "postgresql1" with signal "CONT"

        Then container "postgresql1" pgconsul log contains messages in order within "60" seconds
        """
        psycopg2.OperationalError: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: timeout expired
        Connection timeout diagnostics: pg_status=0
        Skipping.
        """

        # Wait for pgconsul to reconnect successfully — this resets the grace period timer.
        When we wait "5.0" seconds

        # --- Phase 2: second overload — timer must have been reset, so "Skipping." again (not "Forcing action.") ---
        When we kill "postgres" in container "postgresql1" with signal "STOP"
        When we wait "5.0" seconds
        When we kill "postgres" in container "postgresql1" with signal "CONT"

        # The second "Skipping restart" after a successful reconnection proves the counter was reset.
        Then container "postgresql1" pgconsul log contains messages in order within "60" seconds
        """
        psycopg2.OperationalError: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: timeout expired
        Connection timeout diagnostics: pg_status=0
        Skipping.
        Skipping.
        """

        # No failover: lock held, postgres not restarted.
        And zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql1" became a primary
        And postgresql in container "postgresql1" was not restarted
        And "pgbouncer" is running in container "postgresql1"
        And container "postgresql2" is a replica of container "postgresql1"
        And container "postgresql3" is a replica of container "postgresql1"

    @forcing_restart
    Scenario: Overloaded primary IS restarted after exhausting connection timeout budget
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: 'yes'
                    pg_conn_failure_grace_period: 7
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                replica:
                    allow_potential_data_loss: 'no'
                    primary_switch_checks: 1
                    min_failover_timeout: 120
                    primary_unavailability_timeout: 2
                commands:
                    # Wrapper: unfreeze (SIGCONT all postgres), stop, then start — needed because
                    # frozen postgres holds pid file/shared memory, so plain pg_ctl start fails.
                    pg_start: bash -c 'pkill -CONT postgres 2>/dev/null; /usr/bin/postgresql/pg_ctl stop -s -m fast -w -t 10 -D %p 2>/dev/null; exec /usr/bin/postgresql/pg_ctl start -s -w -t %t -D %p --log=/var/log/postgresql/postgresql.log'
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
        And "pgbouncer" is running in container "postgresql1"

        When we remember postgresql start time in container "postgresql1"

        # pg_conn_failure_grace_period=7s. After 7s elapsed → "Forcing action.".
        When we kill "postgres" in container "postgresql1" with signal "STOP"
        Then container "postgresql1" pgconsul log contains messages in order within "60" seconds
        """
        psycopg2.OperationalError: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: timeout expired
        Connection timeout diagnostics: pg_status=0
        Skipping.
        Forcing action.
        Called: stop_pooler
        Called: start_postgresql
        """

        # postgresql1 restarted, remains primary.
        Then postgresql in container "postgresql1" was restarted
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql1" became a primary
        And "pgbouncer" is running in container "postgresql1"
        And container "postgresql2" is a replica of container "postgresql1"
        And container "postgresql3" is a replica of container "postgresql1"

    @skipping_restart
    Scenario: Overloaded replica is not restarted while systemctl reports it running using default pg_conn_failure_grace_period
        # pg_conn_failure_grace_period is NOT set in config — verifies the default value (30.0s) is applied.
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
                    primary_switch_checks: 1
                    min_failover_timeout: 120
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
        And "pgbouncer" is running in container "postgresql2"

        When we remember postgresql start time in container "postgresql2"

        # SIGSTOP simulates overload on replica: process alive (systemctl=running) but connection times out.
        When we kill "postgres" in container "postgresql2" with signal "STOP"

        # pg_conn_failure_grace_period=30s (default). 15s elapsed < 30s → Skipping.
        When we wait "15.0" seconds

        # Unfreeze postgres.
        When we kill "postgres" in container "postgresql2" with signal "CONT"

        Then container "postgresql2" pgconsul log contains messages in order within "60" seconds
        """
        psycopg2.OperationalError: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: timeout expired
        Connection timeout diagnostics: pg_status=0
        Skipping.
        """

        # No failover: primary lock unchanged, replica not restarted.
        And zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And postgresql in container "postgresql2" was not restarted
        And "pgbouncer" is running in container "postgresql2"
        And container "postgresql2" is a replica of container "postgresql1"
        And container "postgresql3" is a replica of container "postgresql1"
