Feature: Replication slots

    @slots
    Scenario Outline: Slots created on promoted replica
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    drop_slot_countdown: 10
                primary:
                    change_replication_type: 'no'
                    primary_switch_checks: 1
                replica:
                    allow_potential_data_loss: 'yes'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 1
                    min_failover_timeout: 1
                    primary_unavailability_timeout: 2
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
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: async
            write_location_diff: 0
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: async
            write_location_diff: 0
        """
        Then container "postgresql1" has following replication slots
        """
          - slot_name: pgconsul_postgresql2_1_pgconsul_pgconsul_net
            slot_type: physical
          - slot_name: pgconsul_postgresql3_1_pgconsul_pgconsul_net
            slot_type: physical
        """
        Then container "postgresql2" has following replication slots
        """
        """
        Then container "postgresql3" has following replication slots
        """
        """
        When we stop container "postgresql1"
        Then container "postgresql2" became a primary
        Then container "postgresql2" has following replication slots
        """
          - slot_name: pgconsul_postgresql1_1_pgconsul_pgconsul_net
            slot_type: physical
          - slot_name: pgconsul_postgresql3_1_pgconsul_pgconsul_net
            slot_type: physical
        """

    Examples: <lock_type>
        |   lock_type   |   lock_host    |
        |   zookeeper   |   zookeeper1   |
