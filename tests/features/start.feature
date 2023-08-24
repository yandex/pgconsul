Feature: Check startup logic

    Scenario Outline: pgconsul restarts without zookeeper
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'no'
                    quorum_commit: '<quorum_commit>'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 1
                    min_failover_timeout: 1
                    primary_unavailability_timeout: 2
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_without_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" without replication slots
        """
            postgresql1:
                role: primary
                config:
                    pgconsul.conf:
                        global:
                            priority: 2
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
                            priority: 3
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql3" is in <replication_type> group
        And <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        When we lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        And we set value "{"hostname": "pgconsul_postgresql1_1.pgconsul_pgconsul_net","timeline": 1}" for key "/pgconsul/postgresql/switchover/master" in <lock_type> "<lock_host>"
        And we set value "scheduled" for key "/pgconsul/postgresql/switchover/state" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        Then container "postgresql3" became a primary
        And container "postgresql2" is a replica of container "postgresql3"
        And container "postgresql1" is a replica of container "postgresql3"
        Then container "postgresql1" is in <replication_type> group
        When we disconnect from network container "postgresql1"
        And we gracefully stop "pgconsul" in container "postgresql1"
        And we start "pgconsul" in container "postgresql1"
        And we wait "40.0" seconds
        And we connect to network container "postgresql1"
        Then container "postgresql1" is in <replication_type> group
        And <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql1_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
        """

    Examples: <lock_type>
        |   lock_type   |   lock_host    | quorum_commit | replication_type |
        |   zookeeper   |   zookeeper1   |      yes      |      quorum      |
        |   zookeeper   |   zookeeper1   |      no       |       sync       |
