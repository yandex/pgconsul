Feature: Testing min_failover_timeout setting

    @failover
    Scenario Outline: Destroy primary and wait min_failover_timeout seconds
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: '<use_slots>'
                    quorum_commit: 'yes'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 1
                    min_failover_timeout: 240
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
                            priority: 2
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 1
        """
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in quorum group
        Then container "postgresql2" is streaming from container "postgresql1"
        And container "postgresql3" is streaming from container "postgresql1"
        When we disconnect from network container "postgresql1"
        Then we save which of "postgresql2,postgresql3" became primary as "new_primary" and the other as "new_replica"
        Then zookeeper "zookeeper1" has holder "pgconsul_new_primary_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then zookeeper "zookeeper1" has value "finished" for key "/pgconsul/postgresql/failover_state"
        Then container "new_replica" is in quorum group
        Then container "new_replica" is streaming from container "new_primary"
        Then container "new_replica" is a replica of container "new_primary"
        Then postgresql in container "new_replica" was not rewinded
        When we connect to network container "postgresql1"
        Then container "new_replica" is streaming from container "new_primary"
        And container "postgresql1" is streaming from container "new_primary"
        Then container "postgresql1" is a replica of container "new_primary"
        Then postgresql in container "postgresql1" was rewinded
        When we disconnect from network container "new_primary"
        Then zookeeper "zookeeper1" has holder "None" for lock "/pgconsul/postgresql/leader"
        Then container "new_replica" is in quorum group
        When we wait until "10.0" seconds to failover of "new_replica" left in zookeeper "zookeeper1"
        Then zookeeper "zookeeper1" has holder "None" for lock "/pgconsul/postgresql/leader"
        Then container "new_replica" is in quorum group
        When we wait "10.0" seconds
        Then zookeeper "zookeeper1" has one of holders "pgconsul_postgresql1_1.pgconsul_pgconsul_net,pgconsul_new_replica_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then one of the containers "postgresql1,new_replica" became a primary, and we remember it
        Then zookeeper "zookeeper1" has value "finished" for key "/pgconsul/postgresql/failover_state"
        Then zookeeper "zookeeper1" has "1" values for key "/pgconsul/postgresql/replics_info"
        When we connect to network container "new_primary"
        Then zookeeper "zookeeper1" has "2" values for key "/pgconsul/postgresql/replics_info"

    Examples: quorum replication <with_slots> slots, disconnect from network/connect to network
        | with_slots | use_slots |
        | without    | no        |
        | with       | yes       |
