Feature: Quorum removal delay

    Scenario: Short replica outage - replica stays in quorum
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
                    quorum_removal_delay: 40
                replica:
                    allow_potential_data_loss: 'no'
                    primary_switch_checks: 3
                    min_failover_timeout: 60
                    primary_unavailability_timeout: 2
        """
        Given a following cluster with "zookeeper" with replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
            postgresql3:
                role: replica
        """
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in quorum group
        Then container "postgresql3" is in quorum group
        
        When we disconnect from network container "postgresql3"
        And we wait "30.0" seconds
        # Replica should remain in quorum (less than 40 seconds passed)
        Then container "postgresql3" is in quorum group
        When we connect to network container "postgresql3"
        And we wait "30.0" seconds
        # Replica returned and should remain in quorum
        Then container "postgresql3" is in quorum group

    Scenario: Long replica outage - replica removed from quorum
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: 'yes'
                primary:
                    change_replication_type: 'yes'
                    quorum_removal_delay: 5
                replica:
                    primary_unavailability_timeout: 1
        """
        Given a following cluster with "zookeeper" with replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
            postgresql3:
                role: replica
        """
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in quorum group
        Then container "postgresql3" is in quorum group
        
        When we disconnect from network container "postgresql3"
        And we wait "8.0" seconds
        
        # Replica should be removed (more than 5 seconds passed)
        Then container "postgresql3" is not in quorum group
        
        When we connect to network container "postgresql3"
        And we wait "5.0" seconds
        
        # Replica returned and should rejoin quorum
        Then container "postgresql3" is in quorum group

    Scenario: Backward compatibility - zero delay removes immediately
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: 'yes'
                primary:
                    change_replication_type: 'yes'
                    quorum_removal_delay: 0
                replica:
                    primary_unavailability_timeout: 1
        """
        Given a following cluster with "zookeeper" with replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
            postgresql3:
                role: replica
        """
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in quorum group
        Then container "postgresql3" is in quorum group
        
        When we disconnect from network container "postgresql3"
        And we wait "3.0" seconds
        
        # With delay=0 replica should be removed immediately (old behavior)
        Then zookeeper "zookeeper1" has value "['pgconsul_postgresql2_1.pgconsul_pgconsul_net']" for key "/pgconsul/postgresql/quorum"
