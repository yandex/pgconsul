Feature: Check not HA hosts

    @failover
    Scenario Outline: Check not ha host from primary
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'no'
                    postgres_timeout: 5
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
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            stream_from: pgconsul_postgresql1_1.pgconsul_pgconsul_net
                stream_from: postgresql1
        """

        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql2" is in <replication_type> group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        When we disconnect from network container "postgresql1"
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql2_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" became a primary
        When we connect to network container "postgresql1"
        Then container "postgresql3" is a replica of container "postgresql1"
        And container "postgresql1" is in <replication_type> group

    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  | quorum_commit | replication_type |
        | zookeeper | zookeeper1 |      yes      |      quorum      |
        | zookeeper | zookeeper1 |      no       |       sync       |

        @failover
    Scenario Outline: Check cascade replica
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'no'
                    postgres_timeout: 5
                    quorum_commit: '<quorum_commit>'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                replica:
                    allow_potential_data_loss: 'no'
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
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            stream_from: pgconsul_postgresql2_1.pgconsul_pgconsul_net
                stream_from: postgresql2
        """

        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql2" is in <replication_type> group
        Then container "postgresql3" is a replica of container "postgresql2"
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
        """
        When we disconnect from network container "postgresql1"
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql2_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" became a primary
        When we connect to network container "postgresql1"
        Then container "postgresql1" is in <replication_type> group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql1_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  | quorum_commit | replication_type |
        | zookeeper | zookeeper1 |      yes      |      quorum      |
        | zookeeper | zookeeper1 |      no       |       sync       |


    @auto_stream_from
    Scenario Outline: Cascade replica streams from primary when replication source fails
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
                    recovery_timeout: 30
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_without_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" without replication slots
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
                            stream_from: pgconsul_postgresql2_1.pgconsul_pgconsul_net
                stream_from: postgresql2
        """

        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql2" is in quorum group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
        """
        Then container "postgresql3" is a replica of container "postgresql2"
        When we disconnect from network container "postgresql2"
        Then container "postgresql3" is a replica of container "postgresql1"
        When we connect to network container "postgresql2"
        Then container "postgresql3" is a replica of container "postgresql2"

    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |


    @auto_stream_from
    Scenario Outline: Cascade replica streams from new primary when old primary fails and it is replication source
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
                    recovery_timeout: 30
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_without_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" without replication slots
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
        And container "postgresql2" is in quorum group
        And <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
        """
        And container "postgresql3" is a replica of container "postgresql1"
        When we disconnect from network container "postgresql1"
        Then container "postgresql2" became a primary
        And container "postgresql3" is a replica of container "postgresql2"
        When we connect to network container "postgresql1"
        Then container "postgresql1" is a replica of container "postgresql2"
        And container "postgresql3" is a replica of container "postgresql1"

    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |


    @auto_stream_from
    Scenario Outline: Cascade replica waits new primary if there are no hosts for streaming in HA
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
                    recovery_timeout: 30
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_without_slot.sh %m %p
            postgresql.conf:
                wal_sender_timeout: '2s'
                wal_receiver_timeout: '2s'
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
        """

        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
        """
        When we remember postgresql start time in container "postgresql2"
        When we disconnect from network container "postgresql1"
        And we wait "10.0" seconds
        When we connect to network container "postgresql1"
        Then postgresql in container "postgresql2" was not restarted
        And postgresql in container "postgresql2" was not rewinded
        Then container "postgresql2" is a replica of container "postgresql1"

    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |



    @auto_stream_from
    Scenario Outline: Cascade replica returns stream from replication source if it is cascade replica too
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
                    recovery_timeout: 30
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_without_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" without replication slots
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
                            stream_from: pgconsul_postgresql2_1.pgconsul_pgconsul_net
                stream_from: postgresql2
            postgresql4:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            stream_from: pgconsul_postgresql3_1.pgconsul_pgconsul_net
                stream_from: postgresql3
        """

        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql2" is in quorum group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
        """
        Then container "postgresql3" is a replica of container "postgresql2"
        When we disconnect from network container "postgresql3"
        Then container "postgresql4" is a replica of container "postgresql1"
        When we connect to network container "postgresql3"
        Then container "postgresql4" is a replica of container "postgresql3"

    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |


    Scenario Outline: Replication slots created automatically
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
                    primary_switch_checks: 1
                    min_failover_timeout: 1
                    primary_unavailability_timeout: 2
                    recovery_timeout: 30
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_without_slot.sh %m %p
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
                            stream_from: pgconsul_postgresql2_1.pgconsul_pgconsul_net
                stream_from: postgresql2
        """

        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql2" is in quorum group
        Then container "postgresql1" has following replication slots
        """
          - slot_name: pgconsul_postgresql2_1_pgconsul_pgconsul_net
            slot_type: physical
        """
        And container "postgresql2" has following replication slots
        """
          - slot_name: pgconsul_postgresql3_1_pgconsul_pgconsul_net
            slot_type: physical
        """
        When we disconnect from network container "postgresql3"
        Then container "postgresql2" has following replication slots
        """
          - slot_name: pgconsul_postgresql3_1_pgconsul_pgconsul_net
            slot_type: physical
        """
        When we wait "10.0" seconds
        Then container "postgresql2" has following replication slots
        """
        """
        And container "postgresql1" has following replication slots
        """
          - slot_name: pgconsul_postgresql2_1_pgconsul_pgconsul_net
            slot_type: physical
        """
        When we connect to network container "postgresql3"
        Then container "postgresql2" has following replication slots
        """
          - slot_name: pgconsul_postgresql3_1_pgconsul_pgconsul_net
            slot_type: physical
        """
        When we disconnect from network container "postgresql2"
        Then container "postgresql1" has following replication slots
        """
          - slot_name: pgconsul_postgresql2_1_pgconsul_pgconsul_net
            slot_type: physical
          - slot_name: pgconsul_postgresql3_1_pgconsul_pgconsul_net
            slot_type: physical
        """
        When we wait "10.0" seconds
        Then container "postgresql1" has following replication slots
        """
          - slot_name: pgconsul_postgresql3_1_pgconsul_pgconsul_net
            slot_type: physical
        """
        When we connect to network container "postgresql2"
        Then container "postgresql1" has following replication slots
        """
          - slot_name: pgconsul_postgresql2_1_pgconsul_pgconsul_net
            slot_type: physical
          - slot_name: pgconsul_postgresql3_1_pgconsul_pgconsul_net
            slot_type: physical
        """
        And container "postgresql2" has following replication slots
        """
          - slot_name: pgconsul_postgresql3_1_pgconsul_pgconsul_net
            slot_type: physical
        """
        When we wait "10.0" seconds
        Then container "postgresql1" has following replication slots
        """
          - slot_name: pgconsul_postgresql2_1_pgconsul_pgconsul_net
            slot_type: physical
        """
        And container "postgresql2" has following replication slots
        """
          - slot_name: pgconsul_postgresql3_1_pgconsul_pgconsul_net
            slot_type: physical
        """
    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |
