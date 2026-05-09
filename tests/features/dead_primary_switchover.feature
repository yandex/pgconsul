Feature: Switchover with dead primary

    @switchover
    Scenario: Check successful switchover with dead primary (no destination)
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    autofailover: 'no'
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
        Then container "postgresql3" is in quorum group
        Then container "postgresql2" is streaming from container "postgresql1"
        And container "postgresql3" is streaming from container "postgresql1"
        When we disconnect from network container "postgresql1"
        And we make switchover task with params "None" in container "postgresql2"
        # We can't make switchover-to with dead primary, so just ignore this option
        Then one of the containers "postgresql2,postgresql3" became a primary, and we remember it
        And another of the containers "postgresql2,postgresql3" is a replica
        And postgresql in another of the containers "postgresql2,postgresql3" was not rewinded
        Then zookeeper "zookeeper1" has value "None" for key "/pgconsul/postgresql/failover_state"
        Then zookeeper "zookeeper1" has "1" values for key "/pgconsul/postgresql/replics_info"
        When we connect to network container "postgresql1"
        Then zookeeper "zookeeper1" has "2" values for key "/pgconsul/postgresql/replics_info"

    @switchover
    Scenario: Check successful switchover with dead primary (with destination)
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    autofailover: 'no'
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
        Then container "postgresql3" is in quorum group
        Then container "postgresql2" is streaming from container "postgresql1"
        And container "postgresql3" is streaming from container "postgresql1"
        When we disconnect from network container "postgresql1"
        And we make switchover task with params "-d pgconsul_postgresql2_1.pgconsul_pgconsul_net" in container "postgresql2"
        # We can't make switchover-to with dead primary, so just ignore this option
        Then one of the containers "postgresql2,postgresql3" became a primary, and we remember it
        And another of the containers "postgresql2,postgresql3" is a replica
        And postgresql in another of the containers "postgresql2,postgresql3" was not rewinded
        Then zookeeper "zookeeper1" has value "None" for key "/pgconsul/postgresql/failover_state"
        Then zookeeper "zookeeper1" has "1" values for key "/pgconsul/postgresql/replics_info"
        When we connect to network container "postgresql1"
        Then zookeeper "zookeeper1" has "2" values for key "/pgconsul/postgresql/replics_info"
