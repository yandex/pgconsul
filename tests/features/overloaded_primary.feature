Feature: Overloaded primary postgres is not restarted by pgconsul

    # Regression test for MDB-46149.
    #
    # Root cause: when pgconsul cannot connect to local postgres due to connect_timeout
    # (e.g. postgres is alive but overloaded), it wrongly assumes postgres is dead and
    # calls dead_iter() which stops odyssey and restarts postgres — unnecessary downtime.
    #
    # Chain of events that triggers the bug:
    #   run_iteration()
    #     → db.get_state()
    #         → is_alive_and_in_terminal_state()
    #             → reconnect()  ← connect_timeout=1 → OperationalError: timeout expired
    #         → raises RuntimeError: PostgreSQL is dead
    #     → get_role() → None
    #     → dead_iter()
    #         → pgpooler('stop')    ← stops odyssey
    #         → start_postgresql()  ← restarts postgres
    #
    # Fix: pgconsul now tracks consecutive connection timeouts (_conn_timeout_count).
    # If the postgres process is alive (get_postgresql_status() == 0) and the counter
    # is below max_conn_timeouts_before_restart, pgconsul logs a warning and skips the
    # restart, returning (True, True) from is_alive_and_in_terminal_state().
    # The connect_timeout grows exponentially (1 → 2 → 4 → 8 → 10 s) so that a
    # temporarily overloaded postgres gets more time to respond on each retry.

    @skipping_restart
    Scenario: Overloaded primary is not restarted while systemctl reports it running
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: 'yes'
                    max_conn_timeouts_before_restart: 5
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

        # Remember the postgres process start time so we can assert it was NOT restarted later.
        When we remember postgresql start time in container "postgresql1"

        # Simulate postgres overload with SIGSTOP: the process is paused but remains visible
        # to systemctl as "running". pgconsul cannot open a new connection (connect_timeout
        # fires), which is indistinguishable from a fully-loaded postgres that is too busy
        # to accept connections.
        When we kill "postgres" in container "postgresql1" with signal "STOP"

        # Wait long enough for pgconsul to accumulate several connection-timeout attempts
        # but well below the total time needed to exhaust the restart threshold.
        # With max_conn_timeouts_before_restart=5 and exponential back-off the full
        # timeout budget is 1+2+4+8+10 = 25 s, so 15 s gives ~3-4 failed attempts.
        When we wait "15.0" seconds

        # Unfreeze postgres — it resumes and starts accepting connections again.
        When we kill "postgres" in container "postgresql1" with signal "CONT"

        # pgconsul must detect that postgres is still alive via systemctl and skip the restart.
        # Then container "postgresql1" pgconsul log contains "Skipping restart"
        Then container "postgresql1" pgconsul log contains messages in order within "60" seconds
        """
        psycopg2.OperationalError: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: timeout expired
        Connection timeout diagnostics: pg_status=0
        Skipping restart
        """

        # The primary ZK lock must still be held — pgconsul keeps running even while
        # postgres is frozen, so no failover should have been triggered.
        And zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"

        # After recovery the cluster must be fully healthy:
        #   - postgresql1 is still the primary (no failover occurred)
        #   - postgres was NOT restarted (start time unchanged)
        #   - odyssey/pgbouncer is still running (dead_iter was never called)
        #   - replicas are streaming from postgresql1
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
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
                    max_conn_timeouts_before_restart: 3
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                replica:
                    allow_potential_data_loss: 'no'
                    primary_switch_checks: 1
                    min_failover_timeout: 120
                    primary_unavailability_timeout: 2
                commands:
                    # pg_start: /usr/bin/postgresql/pg_ctl restart -s -w -t %t -D %p --log=/var/log/postgresql/postgresql.log
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

        # Remember start time — it MUST change after forced restart.
        When we remember postgresql start time in container "postgresql1"

        # Freeze postgres. With max_conn_timeouts_before_restart=3 the exponential
        # timeout budget is 1+2+4 = 7 s. After 3 consecutive timeouts pgconsul
        # should log "Forcing restart" and call dead_iter() unconditionally.
        When we kill "postgres" in container "postgresql1" with signal "STOP"

        # 10 s is well above the 7 s budget plus iteration overhead.
        And we wait "10.0" seconds
        # Unfreeze so that postgres can actually come back up after the restart.
        When we kill "postgres" in container "postgresql1" with signal "CONT"

        # pgconsul must log "Forcing restart" once the counter is exhausted.
        Then container "postgresql1" pgconsul log contains messages in order within "60" seconds
        """
        psycopg2.OperationalError: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: timeout expired
        Connection timeout diagnostics: pg_status=0
        Skipping restart
        Forcing restart
        Called: stop_pooler
        Called: start_postgresql
        """

        When we wait "30.0" seconds

        # After recovery the cluster must be fully healthy:
        #   - postgresql1 is still the primary (no failover occurred)
        #   - odyssey/pgbouncer is still running (dead_iter was never called)
        #   - replicas are streaming from postgresql1
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql1" became a primary
        And "pgbouncer" is running in container "postgresql1"
        And container "postgresql2" is a replica of container "postgresql1"
        And container "postgresql3" is a replica of container "postgresql1"
