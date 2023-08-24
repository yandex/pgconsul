Feature: Test that tests infrastructure works correctly.
         We need to check that all system starts and works
         as expected as is, without any intervention.

    @init
    Scenario: pgconsul container check configuration
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                replica:
                    primary_switch_checks: 100500
            pgbouncer.ini:
                pgbouncer:
                    query_wait_timeout: 100500
                    server_reset_query_always: 2
            postgresql.conf:
                checkpoint_timeout: '30s'
        """
        Given a "zookeeper" container "zookeeper1"
        Given a "zookeeper" container "zookeeper2"
        Given a "zookeeper" container "zookeeper3"
        Given a "backup" container "backup1"
        Given a "pgconsul" container "postgresql1"
        Given a replication slot "pgconsul_postgresql2_1_pgconsul_pgconsul_net" in container "postgresql1"
        Given a "pgconsul" container "postgresql2" with following config
        """
            pgconsul.conf:
                replica:
                    primary_unavailability_timeout: 100500
                    allow_potential_data_loss: 'yes'
            pgbouncer.ini:
                pgbouncer:
                    server_reset_query_always: 1
            postgresql.conf:
                fsync: 'off'
                restore_command: 'rsync -a --password-file=/etc/archive.passwd rsync://archive@pgconsul_backup1_1.pgconsul_pgconsul_net:/archive/%f %p'
        """
        Then container "postgresql2" has following config
        """
            pgconsul.conf:
                replica:
                    primary_unavailability_timeout: 100500
                    allow_potential_data_loss: 'yes'
            pgbouncer.ini:
                pgbouncer:
                    server_reset_query_always: 1
            postgresql.conf:
                fsync: 'off'
                restore_command: 'rsync -a --password-file=/etc/archive.passwd rsync://archive@pgconsul_backup1_1.pgconsul_pgconsul_net:/archive/%f %p'
        """
        Then postgresql in container "postgresql2" has value "off" for option "fsync"
        Then postgresql in container "postgresql2" has value "30s" for option "checkpoint_timeout"
        Then pgbouncer is running in container "postgresql2"
        Then pgbouncer in container "postgresql2" has value "1" for option "server_reset_query_always"
        Then pgbouncer in container "postgresql2" has value "100500" for option "query_wait_timeout"

    Scenario Outline: Check cluster initialization
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    use_replication_slots: 'no'
                    quorum_commit: '<quorum_commit>'
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_without_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" without replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
            postgresql3:
                role: replica
        """
        Then container "postgresql1" is primary
        Then container "postgresql2" is a replica of container "postgresql1"
        Then container "postgresql3" is a replica of container "postgresql1"
        Then pgbouncer is running in container "postgresql1"
        Then pgbouncer is running in container "postgresql2"
        Then pgbouncer is running in container "postgresql3"
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """

    Examples: <lock_type>
        |   lock_type   |   lock_host   | quorum_commit |
        |   zookeeper   |   zookeeper1  |      no       |
        |   zookeeper   |   zookeeper1  |      yes      |
