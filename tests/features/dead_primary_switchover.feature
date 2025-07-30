Feature: Switchover with dead primary

    @switchover
    Scenario Outline: Check successful switchover with dead primary
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    autofailover: 'no'
                    quorum_commit: '<quorum_commit>'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 3
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 3
                    min_failover_timeout: 120
                    primary_unavailability_timeout: 2
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" with replication slots
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
                            priority: 1
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 2

        """
        Then container "postgresql3" is in <replication_type> group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        When we disconnect from network container "postgresql1"
        And we make switchover task with params "<destination>" in container "postgresql2"
        # We can't make switchover-to with dead primary, so just ignore this option
        Then one of the containers "postgresql2,postgresql3" became a primary, and we remember it
        And another of the containers "postgresql2,postgresql3" is a replica
        And postgresql in another of the containers "postgresql2,postgresql3" was not rewinded
        Then <lock_type> "<lock_host>" has value "None" for key "/pgconsul/postgresql/failover_state"
        Then <lock_type> "<lock_host>" has "1" values for key "/pgconsul/postgresql/replics_info"
        When we connect to network container "postgresql1"
        Then <lock_type> "<lock_host>" has "2" values for key "/pgconsul/postgresql/replics_info"
    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |                   destination                   | quorum_commit | replication_type |
        | zookeeper | zookeeper1 |                      None                       |      yes      |      quorum      |
        | zookeeper | zookeeper1 | -d pgconsul_postgresql2_1.pgconsul_pgconsul_net |      yes      |      quorum      |
        | zookeeper | zookeeper1 |                      None                       |      no       |       sync       |
        | zookeeper | zookeeper1 | -d pgconsul_postgresql2_1.pgconsul_pgconsul_net |      no       |       sync       |
