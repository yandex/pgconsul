Feature: Check switchover

    @switchover
    Scenario Outline: Check switchover <restart> restart
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: 'yes'
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
        Given a following cluster with "zookeeper" with replication slots
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
        Then container "postgresql3" is in quorum group
        When we do switchover from container "postgresql1"
        Then container "postgresql3" became a primary
        And container "postgresql2" is a replica of container "postgresql3"
        And container "postgresql1" is a replica of container "postgresql3"
        And postgresql in container "postgresql3" was not restarted
        And postgresql in container "postgresql2" <restarted> restarted
        And postgresql in container "postgresql1" was restarted
        Then container "postgresql1" is in quorum group
        When we do switchover from container "postgresql3"
        Then container "postgresql1" became a primary
        And container "postgresql3" is a replica of container "postgresql1"
        And container "postgresql2" is a replica of container "postgresql1"
        And postgresql in container "postgresql3" was rewinded
        And postgresql in container "postgresql2" was not rewinded
        When we stop container "postgresql2"
        And we do switchover from container "postgresql1"
        And we wait "30.0" seconds
        Then container "postgresql1" is primary
        When we wait "90.0" seconds
        Then container "postgresql3" became a primary
        And container "postgresql1" is a replica of container "postgresql3"
        And timing log in container "postgresql3" contains "switchover,downtime"

    Examples:
        | restart | primary_switch_restart | restarted |
        | with    | yes                    | was       |
        | without | no                     | was not   |

    @switchover_failed_promote
    Scenario: Check failed promote on switchover
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    postgres_timeout: 5
                    switchover_rollback_timeout: 5
                    quorum_commit: 'yes'
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
        Given a following cluster with "zookeeper" with replication slots
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
        Then container "postgresql3" is in quorum group
        When we do switchover from container "postgresql1"
        When we wait "30.0" seconds
        Then container "postgresql1" is primary
        And container "postgresql2" is a replica of container "postgresql1"
        And container "postgresql3" is a replica of container "postgresql1"
        And container "postgresql3" is in quorum group

    @switchover_drop
    Scenario: Incorrect switchover nodes being dropped
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
        Given a following cluster with "zookeeper" with replication slots
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
        When we lock "/pgconsul/postgresql/switchover/lock" in zookeeper "zookeeper1"
        And we set value "{'hostname': null,'timeline': null}" for key "/pgconsul/postgresql/switchover/master" in zookeeper "zookeeper1"
        And we set value "scheduled" for key "/pgconsul/postgresql/switchover/state" in zookeeper "zookeeper1"
        And we release lock "/pgconsul/postgresql/switchover/lock" in zookeeper "zookeeper1"
        Then zookeeper "zookeeper1" has value "None" for key "/pgconsul/postgresql/switchover/master"
        Then zookeeper "zookeeper1" has value "None" for key "/pgconsul/postgresql/switchover/state"
        Then zookeeper "zookeeper1" has value "None" for key "/pgconsul/postgresql/switchover/lsn"
        Then zookeeper "zookeeper1" has value "None" for key "/pgconsul/postgresql/failover_state"
        Then container "postgresql1" is primary
        And container "postgresql2" is a replica of container "postgresql1"
        And container "postgresql3" is a replica of container "postgresql1"


