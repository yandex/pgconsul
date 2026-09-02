Feature: Switchover retries transient candidate failures

    @switchover
    Scenario: A transient failure before promote is retried
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
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 3
                    min_failover_timeout: 120
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
                debug:
                    failure_name: before_promote
                    failure_count: 1
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
        And timing log contains "switchover,downtime"
