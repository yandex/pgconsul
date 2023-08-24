Feature: Asynchronous replication
    Check some cases in mode "change_replication_type = no"


    @failover
    Scenario Outline: No failover in "allow_potential_data_loss = no" mode
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: '<use_slots>'
                    quorum_commit: '<quorum_commit>'
                primary:
                    change_replication_type: 'no'
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
        Given a following cluster with "<lock_type>" <with_slots> replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
            postgresql3:
                role: replica
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: async
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: async
        """
        When we stop container "postgresql1"
        Then <lock_type> "<lock_host>" has holder "None" for lock "/pgconsul/postgresql/leader"
        Then <lock_type> "<lock_host>" has value "None" for key "/pgconsul/postgresql/failover_state"
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: async
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: async
        """
        When we start container "postgresql1"
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: async
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: async
        """
        Then container "postgresql2" is a replica of container "postgresql1"
        Then container "postgresql3" is a replica of container "postgresql1"

    Examples: <lock_type>, <with_slots> replication slots
        | lock_type | lock_host  | with_slots | use_slots | quorum_commit |
        | zookeeper | zookeeper1 |  without   |    no     |      yes      |
        | zookeeper | zookeeper1 |   with     |    yes    |      yes      |
        | zookeeper | zookeeper1 |  without   |    no     |      no       |
        | zookeeper | zookeeper1 |   with     |    yes    |      no       |
