Feature: Reset sync replication without HA replics

    Scenario Outline: Reset sync replication without HA replics
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: '<quorum_commit>'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                    change_replication_metric: count
                replica:
                    allow_potential_data_loss: 'no'
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
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            stream_from: pgconsul_postgresql1_1.pgconsul_pgconsul_net
                stream_from: postgresql1
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in <replication_type> group
        And  <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        When we stop container "postgresql2"
        Then container "postgresql1" is primary
        And  container "postgresql3" is a replica of container "postgresql1"
        When we wait "10.0" seconds
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        And  container "postgresql1" replication state is "async"
        And  postgresql in container "postgresql1" has empty option "synchronous_standby_names"

    Examples: <lock_type>
        | lock_type | lock_host  | quorum_commit | replication_type |
        | zookeeper | zookeeper1 |      yes      |      quorum      |
        | zookeeper | zookeeper1 |      no       |       sync       |

