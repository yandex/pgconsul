Feature: Single node

    Scenario Outline: Single node primary is open with dead ZK
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'no'
                    postgres_timeout: 5
                    election_timeout: 5
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 1
                    min_failover_timeout: 1
                    primary_unavailability_timeout: 2
                    recovery_timeout: 5
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_without_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" without replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            stream_from: pgconsul_postgresql1_1.pgconsul_pgconsul_net
                stream_from: postgresql1
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            stream_from: pgconsul_postgresql1_1.pgconsul_pgconsul_net
                stream_from: postgresql1
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
        When we <destroy> container "zookeeper1"
         And we <destroy> container "zookeeper2"
         And we <destroy> container "zookeeper3"
         And we wait "10.0" seconds
        Then container "postgresql1" is primary
         And pgbouncer is running in container "postgresql1"
         And pgbouncer is running in container "postgresql2"
         And pgbouncer is running in container "postgresql3"
         And container "postgresql2" is a replica of container "postgresql1"
         And container "postgresql3" is a replica of container "postgresql1"
        When we <repair> container "zookeeper1"
         And we <repair> container "zookeeper2"
         And we <repair> container "zookeeper3"
        Then pgbouncer is running in container "postgresql1"
         And pgbouncer is running in container "postgresql2"
         And pgbouncer is running in container "postgresql3"
         And container "postgresql2" is a replica of container "postgresql1"
         And container "postgresql3" is a replica of container "postgresql1"

    Examples: <lock_type>, <lock_host>, <destroy>, <repair>
        | lock_type | lock_host  |          destroy        |       repair       |
        | zookeeper | zookeeper1 |           stop          |        start       |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |

    Scenario Outline: Check async in single node
        Given a "pgconsul" container common config
        """
            postgresql.conf:
                synchronous_standby_names: 'test'
        """
        Given a following cluster with "<lock_type>" without replication slots
        """
            postgresql1:
                role: primary
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And postgresql in container "postgresql1" has empty option "synchronous_standby_names"

    Examples: <lock_type>
        |   lock_type   |   lock_host   |
        |   zookeeper   |   zookeeper1  |
