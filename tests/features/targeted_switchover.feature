Feature: Targeted switchover

    @switchover
    Scenario Outline: Check targeted switchover
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: '<quorum_commit>'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 3
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 3
                    min_failover_timeout: 120
                    primary_unavailability_timeout: 2
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" with replication slots
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
        Then container "postgresql3" is in <replication_type> group
        When we lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        And we set value "{'hostname': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net', 'timeline': 1, 'destination': 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'}" for key "/pgconsul/postgresql/switchover/master" in <lock_type> "<lock_host>"
        And we set value "scheduled" for key "/pgconsul/postgresql/switchover/state" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        Then container "postgresql2" became a primary
        And container "postgresql3" is a replica of container "postgresql2"
        And container "postgresql1" is a replica of container "postgresql2"
        And container "postgresql1" is in <replication_type> group
        And postgresql in container "postgresql3" was not rewinded
        And postgresql in container "postgresql1" was rewinded
        And timing log in container "postgresql2" contains "switchover,downtime"

    Examples: <lock_type>, <lock_host>
        |   lock_type   |   lock_host    | quorum_commit | replication_type |
        |   zookeeper   |   zookeeper1   |      yes      |      quorum      |
        |   zookeeper   |   zookeeper1   |      no       |       sync       |

    @switchover
    Scenario Outline: Host fail targeted switchover
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    postgres_timeout: 20
                    quorum_commit: '<quorum_commit>'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 3
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 3
                    min_failover_timeout: 120
                    primary_unavailability_timeout: 2
                commands:
                    pg_stop: sleep 10 && /usr/bin/postgresql/pg_ctl stop -s -m fast -w -t %t -D %p
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" with replication slots
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
        Then container "postgresql3" is in <replication_type> group
        When we lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        And we set value "{'hostname': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net', 'timeline': 1, 'destination': 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'}" for key "/pgconsul/postgresql/switchover/master" in <lock_type> "<lock_host>"
        And we set value "scheduled" for key "/pgconsul/postgresql/switchover/state" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        And we disconnect from network container "postgresql2"
        And we wait "60.0" seconds
        Then container "postgresql1" is primary
        And container "postgresql3" is a replica of container "postgresql1"
        When we connect to network container "postgresql2"
        And we wait "60.0" seconds
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        And container "postgresql2" is a replica of container "postgresql1"
        When we lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        And we set value "{'hostname': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net', 'timeline': 1, 'destination': 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'}" for key "/pgconsul/postgresql/switchover/master" in <lock_type> "<lock_host>"
        And we set value "scheduled" for key "/pgconsul/postgresql/switchover/state" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        Then container "postgresql2" became a primary
        And container "postgresql3" is a replica of container "postgresql2"
        And container "postgresql1" is a replica of container "postgresql2"
        And container "postgresql1" is in <replication_type> group
        And postgresql in container "postgresql3" was not rewinded
        And postgresql in container "postgresql1" was rewinded

    Examples: <lock_type>, <lock_host>
        |   lock_type   |   lock_host    | quorum_commit | replication_type |
        |   zookeeper   |   zookeeper1   |      yes      |      quorum      |
        |   zookeeper   |   zookeeper1   |      no       |       sync       |

    @switchover
    Scenario Outline: Check targeted switchover with cascade replica
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: '<quorum_commit>'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 3
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 3
                    min_failover_timeout: 120
                    primary_unavailability_timeout: 2
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" with replication slots
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
                            stream_from: pgconsul_postgresql1_1.pgconsul_pgconsul_net
                stream_from: postgresql1

        """
        Then container "postgresql3" is in <replication_type> group
        When we lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        And we set value "{'hostname': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net', 'timeline': 1, 'destination': 'pgconsul_postgresql2_1.pgconsul_pgconsul_net'}" for key "/pgconsul/postgresql/switchover/master" in <lock_type> "<lock_host>"
        And we set value "scheduled" for key "/pgconsul/postgresql/switchover/state" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        Then container "postgresql2" became a primary
        And container "postgresql3" is a replica of container "postgresql1"
        And container "postgresql1" is a replica of container "postgresql2"
        And container "postgresql1" is in <replication_type> group
        And postgresql in container "postgresql3" was not rewinded
        And postgresql in container "postgresql1" was rewinded
        And timing log in container "postgresql2" contains "switchover,downtime"

    Examples: <lock_type>, <lock_host>
        |   lock_type   |   lock_host    | quorum_commit | replication_type |
        |   zookeeper   |   zookeeper1   |      yes      |      quorum      |
        |   zookeeper   |   zookeeper1   |      no       |       sync       |