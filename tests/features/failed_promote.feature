Feature: Destroy new primary after promote and before sync with zookeeper

    @failed_promote
    Scenario Outline: New primary will continue to be primary after restart during promote
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
                    primary_switch_checks: 1
                    min_failover_timeout: 1
                    primary_unavailability_timeout: 2
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_<with_slots>_slot.sh %m %p
                debug:
                    promote_checkpoint_sql: CHECKPOINT; SELECT pg_sleep('infinity');
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
        When we <destroy> container "postgresql1"
        Then we save which of "postgresql2,postgresql3" became primary as "new_primary" and the other as "new_replica"
        Then zookeeper "zookeeper1" has holder "pgconsul_new_primary_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        When we stop container "new_primary"
        When we start container "new_primary"
        Then zookeeper "zookeeper1" has value "finished" for key "/pgconsul/postgresql/failover_state"
        Then container "new_replica" is in quorum group
        Then container "new_replica" is streaming from container "new_primary"
        Then container "new_replica" is a replica of container "new_primary"
        Then postgresql in container "new_replica" was not rewinded
        When we <repair> container "postgresql1"
        Then container "new_replica" is streaming from container "new_primary"
        And container "postgresql1" is streaming from container "new_primary"
        Then container "postgresql1" is a replica of container "new_primary"
        Then pgconsul in container "postgresql1" is connected to zookeeper
        Then postgresql in container "postgresql1" was rewinded

    Examples: quorum replication <with_slots> slots, <destroy>/<repair>
        | with_slots | use_slots |          destroy        |       repair       |
        |  without   |    no     |           stop          |        start       |
        |   with     |    yes    |           stop          |        start       |
        |  without   |    no     | disconnect from network | connect to network |
        |   with     |    yes    | disconnect from network | connect to network |


    @failed_promote_return_primary
    Scenario Outline: New primary will continue to be primary after returning old primary during restart in promote section
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
                    primary_switch_checks: 1
                    min_failover_timeout: 1
                    primary_unavailability_timeout: 2
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_<with_slots>_slot.sh %m %p
                debug:
                    promote_checkpoint_sql: CHECKPOINT; SELECT pg_sleep('infinity');
        """
        Given a following cluster with "zookeeper" <with_slots> replication slots
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
        When we <destroy> container "postgresql1"
        Then we save which of "postgresql2,postgresql3" became primary as "new_primary" and the other as "new_replica"
        Then zookeeper "zookeeper1" has holder "pgconsul_new_primary_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        When we stop container "new_primary"
        When we <repair> container "postgresql1"
        When we start container "new_primary"
        Then zookeeper "zookeeper1" has value "finished" for key "/pgconsul/postgresql/failover_state"
        Then container "postgresql1" is in quorum group
        Then container "new_replica" is streaming from container "new_primary"
        And container "postgresql1" is streaming from container "new_primary"
        Then container "new_replica" is a replica of container "new_primary"
        Then container "postgresql1" is a replica of container "new_primary"
        Then pgconsul in container "postgresql1" is connected to zookeeper
        Then postgresql in container "new_replica" was not rewinded
        Then postgresql in container "postgresql1" was rewinded

    Examples: quorum replication <with_slots> slots, <destroy>/<repair>
        | with_slots | use_slots |          destroy        |       repair       |
        |  without   |    no     |           stop          |        start       |
        |   with     |    yes    |           stop          |        start       |
        |  without   |    no     | disconnect from network | connect to network |
        |   with     |    yes    | disconnect from network | connect to network |
