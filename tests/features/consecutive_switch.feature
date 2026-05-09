Feature: Check that replicas change primary consecutively

    @switchover
    Scenario Outline: Change consecutively on failover
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: '<use_slots>'
                    do_consecutive_primary_switch: 'yes'
                    election_timeout: 10
                    quorum_commit: 'yes'
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
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_<with_slots>_slot.sh %m %p
        """
        Given a following cluster with "zookeeper" <with_slots> replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 4
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 3
            postgresql4:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 2
            postgresql5:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 1
        """
        Then container "postgresql2" is in quorum group
        Then container "postgresql2" is streaming from container "postgresql1"
        And container "postgresql3" is streaming from container "postgresql1"
        And container "postgresql4" is streaming from container "postgresql1"
        And container "postgresql5" is streaming from container "postgresql1"
        When we stop container "postgresql1"
        Then container "postgresql2" became a primary
        Then "3" containers are replicas of "postgresql2" within "120.0" seconds
        And at least "3" postgresql instances were running simultaneously during test
        Then postgresql in container "postgresql3" was not rewinded
        Then postgresql in container "postgresql4" was not rewinded
        Then postgresql in container "postgresql5" was not rewinded

    Examples: <with_slots> replication slots
        | with_slots | use_slots |
        |  without   |    no     |
        |   with     |    yes    |
