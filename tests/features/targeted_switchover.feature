Feature: Targeted switchover

    @switchover
    Scenario: Check targeted switchover
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
        When we do targeted switchover from container "postgresql1" to container "postgresql2"
        Then container "postgresql2" became a primary
        And container "postgresql3" is a replica of container "postgresql2"
        And container "postgresql1" is a replica of container "postgresql2"
        And container "postgresql1" is in quorum group
        And postgresql in container "postgresql3" was not rewinded
        And postgresql in container "postgresql1" was rewinded
        And timing log in container "postgresql2" contains "switchover,downtime"


    @switchover
    Scenario: Host fail targeted switchover
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    postgres_timeout: 20
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
                    pg_stop: sleep 10 && /usr/bin/postgresql/pg_ctl stop -s -m fast -w -t %t -D %p
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
        When we do targeted switchover from container "postgresql1" to container "postgresql2"
        And we disconnect from network container "postgresql2"
        And we wait "60.0" seconds
        Then container "postgresql1" is primary
        And container "postgresql3" is a replica of container "postgresql1"
        When we connect to network container "postgresql2"
        And we wait "60.0" seconds
        Then container "postgresql2" is streaming from container "postgresql1"
        And container "postgresql3" is streaming from container "postgresql1"
        And container "postgresql2" is a replica of container "postgresql1"
        When we do targeted switchover from container "postgresql1" to container "postgresql2"
        Then container "postgresql2" became a primary
        And container "postgresql3" is a replica of container "postgresql2"
        And container "postgresql1" is a replica of container "postgresql2"
        And container "postgresql1" is in quorum group
        And postgresql in container "postgresql3" was not rewinded
        And postgresql in container "postgresql1" was rewinded
