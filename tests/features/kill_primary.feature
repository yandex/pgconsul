Feature: Destroy primary in various scenarios


    @failover
    Scenario Outline: Destroy primary on 2-hosts cluster with primary_switch_restart = <primary_switch_restart>
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
        Given a following cluster with "<lock_type>" <with_slots> replication slots
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
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in quorum group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
        """
        When we <destroy> container "postgresql1"
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql2_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" became a primary
        Then <lock_type> "<lock_host>" has value "finished" for key "/pgconsul/postgresql/failover_state"
    Examples: <lock_type>, synchronous replication <with_slots> slots, <destroy>/<repair>
        | lock_type | lock_host  |          destroy        |       repair       | with_slots | use_slots | primary_switch_restart |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |   with     |    yes    |        no        |


    @failover
    Scenario Outline: Destroy primary one by one with primary_switch_restart = <primary_switch_restart>
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
        Given a following cluster with "<lock_type>" <with_slots> replication slots
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
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in quorum group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        When we <destroy> container "postgresql1"
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql2_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" became a primary
        Then <lock_type> "<lock_host>" has value "finished" for key "/pgconsul/postgresql/failover_state"
        Then container "postgresql3" is in quorum group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        Then container "postgresql3" is a replica of container "postgresql2"
        Then postgresql in container "postgresql3" was not rewinded
        Then <lock_type> "<lock_host>" has value "['pgconsul_postgresql3_1.pgconsul_pgconsul_net']" for key "/pgconsul/postgresql/quorum"
        When we <destroy> container "postgresql2"
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql3_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql3" became a primary
    Examples: <lock_type>, synchronous replication <with_slots> slots, <destroy>/<repair>
        | lock_type | lock_host  |          destroy        |       repair       | with_slots | use_slots | quorum_commit | replication_type | primary_switch_restart |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |   with     |    yes    |      yes      |      quorum      |        no        |


    @failover
    Scenario Outline: Destroy primary with primary_switch_restart = <primary_switch_restart>
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: '<use_slots>'
                    quorum_commit: '<quorum_commit>'
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
        Given a following cluster with "<lock_type>" <with_slots> replication slots
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
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in <replication_type> group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        When we <destroy> container "postgresql1"
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql2_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" became a primary
        Then <lock_type> "<lock_host>" has value "finished" for key "/pgconsul/postgresql/failover_state"
        Then container "postgresql3" is in <replication_type> group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        Then container "postgresql3" is a replica of container "postgresql2"
        Then postgresql in container "postgresql3" was not rewinded
        When we <repair> container "postgresql1"
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql1_1.pgconsul_pgconsul_net
            state: streaming
        """
        Then container "postgresql1" is a replica of container "postgresql2"
        Then pgconsul in container "postgresql1" is connected to zookeeper
        Then postgresql in container "postgresql1" was rewinded

    Examples: <lock_type>, synchronous replication <with_slots> slots, <destroy>/<repair>
        | lock_type | lock_host  |          destroy        |       repair       | with_slots | use_slots | quorum_commit | replication_type | primary_switch_restart |
        | zookeeper | zookeeper1 |           stop          |        start       |  without   |    no     |      yes      |      quorum      |        yes       |
        | zookeeper | zookeeper1 |           stop          |        start       |   with     |    yes    |      yes      |      quorum      |        yes       |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |  without   |    no     |      yes      |      quorum      |        yes       |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |   with     |    yes    |      yes      |      quorum      |        yes       |
        | zookeeper | zookeeper1 |           stop          |        start       |  without   |    no     |      no       |       sync       |        yes       |
        | zookeeper | zookeeper1 |           stop          |        start       |   with     |    yes    |      no       |       sync       |        yes       |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |  without   |    no     |      no       |       sync       |        yes       |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |   with     |    yes    |      no       |       sync       |        yes       |
        | zookeeper | zookeeper1 |           stop          |        start       |  without   |    no     |      yes      |      quorum      |        no        |
        | zookeeper | zookeeper1 |           stop          |        start       |   with     |    yes    |      yes      |      quorum      |        no        |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |  without   |    no     |      yes      |      quorum      |        no        |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |   with     |    yes    |      yes      |      quorum      |        no        |
        | zookeeper | zookeeper1 |           stop          |        start       |  without   |    no     |      no       |       sync       |        no        |
        | zookeeper | zookeeper1 |           stop          |        start       |   with     |    yes    |      no       |       sync       |        no        |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |  without   |    no     |      no       |       sync       |        no        |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |   with     |    yes    |      no       |       sync       |        no        |


    @failover
    Scenario Outline: Destroy primary with async replication
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: '<use_slots>'
                primary:
                    change_replication_type: '<change_replication>'
                    primary_switch_checks: 1
                replica:
                    allow_potential_data_loss: '<data_loss>'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 1
                    min_failover_timeout: 1
                    primary_unavailability_timeout: 2
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_<with_slots>_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" <with_slots> replication slots
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
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql2_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/sync_replica"
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: <sync_state>
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: async
        """
        When we <destroy> container "postgresql1"
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql2_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" became a primary
        Then <lock_type> "<lock_host>" has value "finished" for key "/pgconsul/postgresql/failover_state"
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql3_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/sync_replica"
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: async
        """
        Then container "postgresql3" is a replica of container "postgresql2"
        Then postgresql in container "postgresql3" was not rewinded
        When we <repair> container "postgresql1"
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: <sync_state>
          - client_hostname: pgconsul_postgresql1_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: async
        """
        Then container "postgresql1" is a replica of container "postgresql2"
        Then pgconsul in container "postgresql1" is connected to zookeeper
        Then postgresql in container "postgresql1" was rewinded

        Examples: <lock_type>, <sync_state>hronous replication <with_slots> slots, <destroy>/<repair>
            | lock_type | lock_host  |          destroy        |       repair       | with_slots | use_slots | sync_state | change_replication | data_loss |
            | zookeeper | zookeeper1 |           stop          |        start       |  without   |    no     |    async   |        no          |    yes    |
            | zookeeper | zookeeper1 |           stop          |        start       |   with     |    yes    |    async   |        no          |    yes    |
            | zookeeper | zookeeper1 | disconnect from network | connect to network |  without   |    no     |    async   |        no          |    yes    |
            | zookeeper | zookeeper1 | disconnect from network | connect to network |   with     |    yes    |    async   |        no          |    yes    |


    @failover_archive
    Scenario Outline: Destroy primary with one replica in archive recovery
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
                    primary_switch_checks: 3
                    min_failover_timeout: 1
                    primary_unavailability_timeout: 2
                    primary_switch_restart: 'no'
                    recovery_timeout: 20
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" with replication slots
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
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in quorum group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        When we set value "no" for option "use_replication_slots" in section "global" in pgconsul config in container "postgresql3"
        And we restart "pgconsul" in container "postgresql3"
        When we stop container "postgresql3"
        And we wait "10.0" seconds
        Then container "postgresql1" has following replication slots
        """
          - slot_name: pgconsul_postgresql2_1_pgconsul_pgconsul_net
            slot_type: physical
        """
        When we start container "postgresql3"
        Then <lock_type> "<lock_host>" has value "['pgconsul_postgresql2_1.pgconsul_pgconsul_net']" for key "/pgconsul/postgresql/quorum"
        When we wait "10.0" seconds
        When we <destroy> container "postgresql1"
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql2_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" became a primary
        Then <lock_type> "<lock_host>" has value "finished" for key "/pgconsul/postgresql/failover_state"
        When we set value "yes" for option "use_replication_slots" in section "global" in pgconsul config in container "postgresql3"
        And we restart "pgconsul" in container "postgresql3"
        Then container "postgresql3" is in quorum group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        Then container "postgresql3" is a replica of container "postgresql2"
        Then postgresql in container "postgresql3" was not rewinded
        When we <repair> container "postgresql1"
        Then container "postgresql1" is a replica of container "postgresql2"
        Then pgconsul in container "postgresql1" is connected to zookeeper
        Then postgresql in container "postgresql1" was rewinded
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql1_1.pgconsul_pgconsul_net
            state: streaming
        """
    Examples: <lock_type>, synchronous replication with slots, <destroy>/<repair>
        | lock_type | lock_host  |          destroy        |       repair       |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |
        | zookeeper | zookeeper1 |           stop          |        start       |
