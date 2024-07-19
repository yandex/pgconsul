Feature: Check switchover

    @switchover
    Scenario Outline: Check switchover <restart> restart
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
                    primary_switch_restart: '<primary_switch_restart>'
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
        When we remember postgresql start time in container "postgresql1"
        When we remember postgresql start time in container "postgresql2"
        When we remember postgresql start time in container "postgresql3"
        Then container "postgresql3" is in <replication_type> group
        When we lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        And we set value "{'hostname': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net','timeline': 1}" for key "/pgconsul/postgresql/switchover/master" in <lock_type> "<lock_host>"
        And we set value "scheduled" for key "/pgconsul/postgresql/switchover/state" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        Then container "postgresql3" became a primary
        And container "postgresql2" is a replica of container "postgresql3"
        And container "postgresql1" is a replica of container "postgresql3"
        And postgresql in container "postgresql3" was not restarted
        And postgresql in container "postgresql2" <restarted> restarted
        And postgresql in container "postgresql1" was restarted
        Then container "postgresql1" is in <replication_type> group
        When we lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        And we set value "{'hostname': 'pgconsul_postgresql3_1.pgconsul_pgconsul_net','timeline': 2}" for key "/pgconsul/postgresql/switchover/master" in <lock_type> "<lock_host>"
        And we set value "scheduled" for key "/pgconsul/postgresql/switchover/state" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        Then container "postgresql1" became a primary
        And container "postgresql3" is a replica of container "postgresql1"
        And container "postgresql2" is a replica of container "postgresql1"
        And postgresql in container "postgresql3" was not rewinded
        And postgresql in container "postgresql2" was not rewinded
        When we stop container "postgresql2"
        And we lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        And we set value "{'hostname': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net','timeline': 3}" for key "/pgconsul/postgresql/switchover/master" in <lock_type> "<lock_host>"
        And we set value "scheduled" for key "/pgconsul/postgresql/switchover/state" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        And we wait "30.0" seconds
        Then container "postgresql1" is primary
        When we wait "90.0" seconds
        Then container "postgresql3" became a primary
        And container "postgresql1" is a replica of container "postgresql3"

    Examples: <lock_type>, <lock_host>
        |   lock_type   |   lock_host    | quorum_commit | replication_type | restart | primary_switch_restart | restarted |
        |   zookeeper   |   zookeeper1   |      yes      |      quorum      |  with   |        yes       |    was    |
        |   zookeeper   |   zookeeper1   |      no       |       sync       |  with   |        yes       |    was    |
        |   zookeeper   |   zookeeper1   |      yes      |      quorum      | without |        no        |  was not  |
        |   zookeeper   |   zookeeper1   |      no       |       sync       | without |        no        |  was not  |


    Scenario Outline: Check failed promote on switchover
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    postgres_timeout: 5
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
                    recovery_timeout: 5
                commands:
                    promote: sleep 3 && false
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" with replication slots
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
        Then container "postgresql3" is in <replication_type> group
        When we lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        And we set value "{'hostname': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net','timeline': 1}" for key "/pgconsul/postgresql/switchover/master" in <lock_type> "<lock_host>"
        And we set value "scheduled" for key "/pgconsul/postgresql/switchover/state" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        When we wait "30.0" seconds
        Then container "postgresql1" is primary
        And container "postgresql2" is a replica of container "postgresql1"
        And container "postgresql3" is a replica of container "postgresql1"
        And container "postgresql3" is in <replication_type> group

    Examples: <lock_type>, <lock_host>
        |   lock_type   |   lock_host    | quorum_commit | replication_type |
        |   zookeeper   |   zookeeper1   |      yes      |      quorum      |
        |   zookeeper   |   zookeeper1   |      no       |       sync       |


    @switchover_drop
    Scenario Outline: Incorrect switchover nodes being dropped
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
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
        When we lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        And we set value "{'hostname': null,'timeline': null}" for key "/pgconsul/postgresql/switchover/master" in <lock_type> "<lock_host>"
        And we set value "scheduled" for key "/pgconsul/postgresql/switchover/state" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        Then <lock_type> "zookeeper1" has value "None" for key "/pgconsul/postgresql/switchover/master"
        Then <lock_type> "zookeeper1" has value "None" for key "/pgconsul/postgresql/switchover/state"
        Then <lock_type> "zookeeper1" has value "None" for key "/pgconsul/postgresql/switchover/lsn"
        Then <lock_type> "zookeeper1" has value "None" for key "/pgconsul/postgresql/failover_state"
        Then container "postgresql1" is primary
        And container "postgresql2" is a replica of container "postgresql1"
        And container "postgresql3" is a replica of container "postgresql1"

    Examples: <lock_type>, <lock_host>
        |   lock_type   |   lock_host    |
        |   zookeeper   |   zookeeper1   |
