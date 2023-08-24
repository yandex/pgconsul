Feature: Check disable sync replication
    Scenario Outline: Disable sync replication when overload
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: yes
                    quorum_commit: '<quorum_commit>'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                    weekday_change_hours: 0-24
                    weekend_change_hours: 0-24
                    overload_sessions_ratio: 50
                    change_replication_metric: count,time,load
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
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql3" is in <replication_type> group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        Then container "postgresql1" is primary
        When run in container "postgresql1" "88" sessions with timeout 3600
        Then postgresql in container "postgresql1" has empty option "synchronous_standby_names"


    Examples: <lock_type>  <destroy>/<repair>
        | lock_type | lock_host  | quorum_commit | replication_type |
        | zookeeper | zookeeper1 |      yes      |      quorum      |
        | zookeeper | zookeeper1 |      no       |       sync       |


    Scenario Outline: Destroy all replicas when time to change async is possible
        Given a "pgconsul" container common config:
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: yes
                    quorum_commit: '<quorum_commit>'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                    weekday_change_hours: 0-24
                    weekend_change_hours: 0-24
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 1
                    min_failover_timeout: 1
                    primary_unavailability_timeout: 2
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" with replication slots:
        """
            postgresql1:
                role: primary
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
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql3" is in <replication_type> group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info":
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        When we <destroy> container "postgresql3"
        And  we <destroy> container "postgresql2"
        Then container "postgresql1" is primary
        Then postgresql in container "postgresql1" has empty option "synchronous_standby_names"
        When we <repair> container "postgresql3"
        When we <repair> container "postgresql2"
        Then container "postgresql3" is a replica of container "postgresql1"
        Then container "postgresql2" is a replica of container "postgresql1"
        Then container "postgresql3" is in <replication_type> group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info":
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """

    Examples: <lock_type>  <destroy>/<repair>
        | lock_type | lock_host  |          destroy        |       repair       | quorum_commit | replication_type |
        | zookeeper | zookeeper1 |           stop          |        start       |      yes      |      quorum      |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |      yes      |      quorum      |
        | zookeeper | zookeeper1 |           stop          |        start       |      no       |       sync       |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |      no       |       sync       |

