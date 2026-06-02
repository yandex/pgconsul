Feature: Destroy primary in various scenarios


    @failover
    Scenario: Destroy primary on 2-hosts cluster with primary_switch_restart = no
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
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
                    primary_switch_restart: no
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "zookeeper" with replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 2
        """
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in quorum group
        Then container "postgresql2" is streaming from container "postgresql1"
        When we disconnect from network container "postgresql1"
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql2_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" became a primary
        Then zookeeper "zookeeper1" has value "finished" for key "/pgconsul/postgresql/failover_state"
        And timing log in container "postgresql2" contains "failover,downtime"


    @failover
    Scenario: Destroy primary one by one with primary_switch_restart = no
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
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
                    primary_switch_restart: no
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "zookeeper" with replication slots
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
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql2_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" became a primary
        Then zookeeper "zookeeper1" has value "finished" for key "/pgconsul/postgresql/failover_state"
        And timing log in container "postgresql2" contains "failover,downtime"
        Then container "postgresql3" is in quorum group
        Then container "postgresql3" is streaming from container "postgresql2"
        Then container "postgresql3" is a replica of container "postgresql2"
        Then postgresql in container "postgresql3" was not rewinded
        Then zookeeper "zookeeper1" has value "['pgconsul_postgresql3_1.pgconsul_pgconsul_net']" for key "/pgconsul/postgresql/quorum"
        When we disconnect from network container "postgresql2"
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql3_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql3" became a primary
        Then zookeeper "zookeeper1" has value "finished" for key "/pgconsul/postgresql/failover_state"
        And timing log in container "postgresql3" contains "failover,downtime"


    @failover @focus
    Scenario Outline: Destroy primary with primary_switch_restart = <primary_switch_restart>
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
                    min_failover_timeout: 1
                    primary_unavailability_timeout: 2
                    primary_switch_restart: <primary_switch_restart>
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
        When we <destroy> container "postgresql1"
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql2_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" became a primary
        Then zookeeper "zookeeper1" has value "finished" for key "/pgconsul/postgresql/failover_state"
        And timing log in container "postgresql2" contains "failover,downtime"
        Then container "postgresql3" is in quorum group
        Then container "postgresql3" is streaming from container "postgresql2"
        Then container "postgresql3" is a replica of container "postgresql2"
        Then postgresql in container "postgresql3" was not rewinded
        When we <repair> container "postgresql1"
        Then zookeeper "zookeeper1" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql1_1.pgconsul_pgconsul_net
            state: streaming
        """
        Then container "postgresql1" is a replica of container "postgresql2"
        Then pgconsul in container "postgresql1" is connected to zookeeper
        Then postgresql in container "postgresql1" was rewinded

    Examples: synchronous replication <with_slots> slots, <destroy>/<repair>
        | destroy                 | repair             | with_slots | use_slots | primary_switch_restart |
        | stop                    | start              | without    | no        | yes                    |
        | stop                    | start              | with       | yes       | yes                    |
        | disconnect from network | connect to network | without    | no        | yes                    |
        | disconnect from network | connect to network | with       | yes       | yes                    |
        | stop                    | start              | without    | no        | no                     |
        | stop                    | start              | with       | yes       | no                     |
        | disconnect from network | connect to network | without    | no        | no                     |
        | disconnect from network | connect to network | with       | yes       | no                     |


    @failover_archive
    Scenario Outline: Destroy primary with one replica in archive recovery
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: 'yes'
                    drop_slot_countdown: 10
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 3
                    min_failover_timeout: 1
                    primary_unavailability_timeout: 2
                    primary_switch_restart: 'no'
                    recovery_timeout: 20
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "zookeeper" with replication slots
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
        When we set value "no" for option "replication_slots_polling" in section "global" in pgconsul config in container "postgresql3"
        And we restart "pgconsul" in container "postgresql3"
        When we stop container "postgresql3"
        And we wait "10.0" seconds
        Then container "postgresql1" has following replication slots
        """
          - slot_name: pgconsul_postgresql2_1_pgconsul_pgconsul_net
            slot_type: physical
        """
        When we start container "postgresql3"
        Then zookeeper "zookeeper1" has value "['pgconsul_postgresql2_1.pgconsul_pgconsul_net']" for key "/pgconsul/postgresql/quorum"
        When we wait "10.0" seconds
        When we <destroy> container "postgresql1"
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql2_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" became a primary
        Then zookeeper "zookeeper1" has value "finished" for key "/pgconsul/postgresql/failover_state"
        And timing log in container "postgresql2" contains "failover,downtime"
        When we set value "yes" for option "replication_slots_polling" in section "global" in pgconsul config in container "postgresql3"
        And we restart "pgconsul" in container "postgresql3"
        Then container "postgresql3" is in quorum group
        Then container "postgresql3" is streaming from container "postgresql2"
        Then container "postgresql3" is a replica of container "postgresql2"
        Then postgresql in container "postgresql3" was not rewinded
        When we <repair> container "postgresql1"
        Then container "postgresql1" is a replica of container "postgresql2"
        Then pgconsul in container "postgresql1" is connected to zookeeper
        Then postgresql in container "postgresql1" was rewinded
        Then container "postgresql3" is streaming from container "postgresql2"
        And container "postgresql1" is streaming from container "postgresql2"
    Examples: synchronous replication with slots, <destroy>/<repair>
        |          destroy        |       repair       |
        | disconnect from network | connect to network |
        |           stop          |        start       |
