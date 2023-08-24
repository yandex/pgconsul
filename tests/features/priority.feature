Feature: Replicas priority



    Scenario Outline: Asynchronous replica with higher priority promoted if replicas have same LSN
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
                    allow_potential_data_loss: 'yes'
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
                            priority: 1
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 2
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
            write_location_diff: 0
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
            write_location_diff: 0
        """
        When we stop container "postgresql1"
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql3_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql3" became a primary
        Then <lock_type> "<lock_host>" has value "finished" for key "/pgconsul/postgresql/failover_state"
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
        """
        Then container "postgresql2" is a replica of container "postgresql3"
        When we start container "postgresql1"
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql1_1.pgconsul_pgconsul_net
            state: streaming
        """
        Then container "postgresql1" is a replica of container "postgresql3"

    Examples: <lock_type>, <with_slots> replication slots
        |   lock_type   |   lock_host   |   with_slots  |   use_slots   | quorum_commit |
        |   zookeeper   |   zookeeper1  |     without   |       no      |      yes      |
        |   zookeeper   |   zookeeper1  |      with     |       yes     |      yes      |
        |   zookeeper   |   zookeeper1  |     without   |       no      |      no       |
        |   zookeeper   |   zookeeper1  |      with     |       yes     |      no       |




    Scenario Outline: Change synchronous replicas priority
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
        When we set value "10" for option "priority" in section "global" in pgconsul config in container "postgresql2"
        And we restart "pgconsul" in container "postgresql2"
        Then container "postgresql2" is in <replication_type> group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        When we set value "1" for option "priority" in section "global" in pgconsul config in container "postgresql2"
        And we restart "pgconsul" in container "postgresql2"
        Then container "postgresql3" is in <replication_type> group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """

    Examples: <lock_type>, <with_slots> replication slots
        |   lock_type   |   lock_host   |   with_slots  |   use_slots   | quorum_commit | replication_type |
        |   zookeeper   |   zookeeper1  |    without    |       no      |      yes      |      quorum      |
        |   zookeeper   |   zookeeper1  |     with      |       yes     |      yes      |      quorum      |
        |   zookeeper   |   zookeeper1  |    without    |       no      |      no       |       sync       |
        |   zookeeper   |   zookeeper1  |     with      |       yes     |      no       |       sync       |



    Scenario Outline: Missing priority key is always filled from config
        Given a "pgconsul" container common config
        """
             pgconsul.conf:
                 global:
                     priority: 10
        """
        And a following cluster with "<lock_type>" without replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
            postgresql3:
                role: replica
        """
        When we remove key "/pgconsul/postgresql/all_hosts/pgconsul_postgresql2_1.pgconsul_pgconsul_net/prio" in <lock_type> "<lock_host>"
        And we remove key "/pgconsul/postgresql/all_hosts/pgconsul_postgresql1_1.pgconsul_pgconsul_net/prio" in <lock_type> "<lock_host>"
        Then <lock_type> "<lock_host>" has value "10" for key "/pgconsul/postgresql/all_hosts/pgconsul_postgresql1_1.pgconsul_pgconsul_net/prio"
        And <lock_type> "<lock_host>" has value "10" for key "/pgconsul/postgresql/all_hosts/pgconsul_postgresql2_1.pgconsul_pgconsul_net/prio"

    Examples: <lock_type>
        |   lock_type   |   lock_host   |
        |   zookeeper   |   zookeeper1  |
