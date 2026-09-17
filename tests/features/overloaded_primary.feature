Feature: Overloaded postgres is not restarted prematurely by pgconsul

    # Regression test for MDB-46149. SIGSTOP keeps the process running while
    # making protocol connections time out.

    @skipping_restart @skipping_restart_primary
    Scenario: A briefly overloaded primary is not restarted and a successful connection resets the grace period
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
                    primary_unavailability_timeout: 10
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

        When we kill "postgres" in container "postgresql1" with signal "STOP"
        When we wait "15.0" seconds
        When we kill "postgres" in container "postgresql1" with signal "CONT"

        Then container "postgresql1" pgconsul log contains messages in order within "60" seconds
        """
        psycopg2.OperationalError: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: timeout expired
        Connection timeout diagnostics: pg_status=0
        Skipping.
        """

        When we wait "5.0" seconds
        When we kill "postgres" in container "postgresql1" with signal "STOP"
        When we wait "15.0" seconds
        When we kill "postgres" in container "postgresql1" with signal "CONT"

        Then container "postgresql1" pgconsul log contains messages in order within "60" seconds
        """
        psycopg2.OperationalError: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: timeout expired
        Connection timeout diagnostics: pg_status=0
        Skipping.
        Skipping.
        """

        And zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql1" became a primary
        And postgresql in container "postgresql1" was not restarted
        And "pgbouncer" is running in container "postgresql1"
        And container "postgresql2" is a replica of container "postgresql1"
        And container "postgresql3" is a replica of container "postgresql1"

    @forcing_restart
    Scenario: A continuously overloaded primary is restarted after the grace period
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
                    # Unfreeze the stopped process before restarting it.
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

        Then postgresql in container "postgresql1" was restarted
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql1" became a primary
        And "pgbouncer" is running in container "postgresql1"
        And container "postgresql2" is a replica of container "postgresql1"
        And container "postgresql3" is a replica of container "postgresql1"
