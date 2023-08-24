Feature: Check pgconsul-util features

    @pgconsul_util_maintenance
    Scenario Outline: Check pgconsul-util maintenance works
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
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" with replication slots
        """
            postgresql1:
                role: primary
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        When we run following command on host "postgresql1"
        """
        pgconsul-util maintenance -m show
        """
        Then command exit with return code "0"
        And command result contains following output
        """
        disabled
        """
        When we run following command on host "postgresql1"
        """
        pgconsul-util maintenance -m enable
        """
        Then command exit with return code "0"
        And <lock_type> "<lock_host>" has value "enable" for key "/pgconsul/postgresql/maintenance"
        When we run following command on host "postgresql1"
        """
        pgconsul-util maintenance -m show
        """
        Then command exit with return code "0"
        And command result contains following output
        """
        enabled
        """
        When we run following command on host "postgresql1"
        """
        pgconsul-util maintenance -m disable
        """
        Then command exit with return code "0"
        And <lock_type> "<lock_host>" has value "None" for key "/pgconsul/postgresql/maintenance"
        When we run following command on host "postgresql1"
        """
        pgconsul-util maintenance -m show
        """
        Then command exit with return code "0"
        And command result contains following output
        """
        disabled
        """
    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |


    @pgconsul_util_maintenance
    Scenario Outline: Check pgconsul-util maintenance enable with wait_all option works fails
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
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" with replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql2" is in quorum group
        When we gracefully stop "pgconsul" in container "postgresql2"
        And we gracefully stop "pgconsul" in container "postgresql1"
        And we lock "/pgconsul/postgresql/alive/pgconsul_postgresql1_1.pgconsul_pgconsul_net" in <lock_type> "<lock_host>"
        And we lock "/pgconsul/postgresql/alive/pgconsul_postgresql2_1.pgconsul_pgconsul_net" in <lock_type> "<lock_host>"
        When we run following command on host "postgresql1"
        """
        pgconsul-util maintenance -m enable --wait_all --timeout 10
        """
        Then command exit with return code "1"
        And command result contains following output
        """
        TimeoutError
        """
        When we release lock "/pgconsul/postgresql/alive/pgconsul_postgresql1_1.pgconsul_pgconsul_net" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/alive/pgconsul_postgresql2_1.pgconsul_pgconsul_net" in <lock_type> "<lock_host>"
    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |


    @pgconsul_util_maintenance
    Scenario Outline: Check pgconsul-util maintenance with wait_all option works works
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
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" with replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql2" is in quorum group
        When we run following command on host "postgresql1"
        """
        pgconsul-util maintenance -m enable --wait_all --timeout 10
        """
        Then command exit with return code "0"
        And command result contains following output
        """
        Success
        """
        And <lock_type> "<lock_host>" has value "enable" for key "/pgconsul/postgresql/maintenance"
        When we run following command on host "postgresql1"
        """
        pgconsul-util maintenance -m disable --wait_all --timeout 10
        """
        Then command exit with return code "0"
        And command result contains following output
        """
        Success
        """
        And <lock_type> "<lock_host>" has value "None" for key "/pgconsul/postgresql/maintenance"
    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |


    @pgconsul_util_maintenance
    Scenario Outline: Check pgconsul-util maintenance disable with wait_all option works fails
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
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" with replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql2" is in quorum group
        When we run following command on host "postgresql1"
        """
        pgconsul-util maintenance -m enable --wait_all --timeout 10
        """
        Then command exit with return code "0"
        And command result contains following output
        """
        Success
        """
        And <lock_type> "<lock_host>" has value "enable" for key "/pgconsul/postgresql/maintenance"
        When we gracefully stop "pgconsul" in container "postgresql2"
        And we gracefully stop "pgconsul" in container "postgresql1"
        And we lock "/pgconsul/postgresql/alive/pgconsul_postgresql1_1.pgconsul_pgconsul_net" in <lock_type> "<lock_host>"
        And we lock "/pgconsul/postgresql/alive/pgconsul_postgresql2_1.pgconsul_pgconsul_net" in <lock_type> "<lock_host>"
        When we run following command on host "postgresql1"
        """
        pgconsul-util maintenance -m disable --wait_all --timeout 10
        """
        Then command exit with return code "1"
        And command result contains following output
        """
        TimeoutError
        """
        When we release lock "/pgconsul/postgresql/alive/pgconsul_postgresql1_1.pgconsul_pgconsul_net" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/alive/pgconsul_postgresql2_1.pgconsul_pgconsul_net" in <lock_type> "<lock_host>"
    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |


    @pgconsul_util_switchover_single
    Scenario Outline: Check pgconsul-util switchover single-node cluster works as expected
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
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" with replication slots
        """
            postgresql1:
                role: primary
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        When we run following command on host "postgresql1"
        """
        pgconsul-util switchover --yes --block
        """
        Then command exit with return code "1"
        And command result contains following output
        """
        Switchover is impossible now
        """
    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |


    @pgconsul_util_switchover_stream_from
    Scenario Outline: Check pgconsul-util switchover single-node cluster works as expected
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
                            stream_from: pgconsul_postgresql1_1.pgconsul_pgconsul_net
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        When we run following command on host "postgresql1"
        """
        pgconsul-util switchover --yes --block
        """
        Then command exit with return code "1"
        And command result contains following output
        """
        Switchover is impossible now
        """
    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |


    @pgconsul_util_switchover
    Scenario Outline: Check pgconsul-util switchover works as expected
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
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in <replication_type> group
        When we run following command on host "postgresql1"
        """
        pgconsul-util switchover --yes --block
        """
        Then command exit with return code "0"
        And command result contains following output
        """
        switchover finished, zk status "None"
        """
        Then container "postgresql2" became a primary
        And container "postgresql1" is a replica of container "postgresql2"
        And container "postgresql1" is in <replication_type> group
        And postgresql in container "postgresql1" was not rewinded
    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  | replication_type | quorum_commit |
        | zookeeper | zookeeper1 |      sync        |       no      |
        | zookeeper | zookeeper1 |      quorum      |      yes      |


    @pgconsul_util_switchover
    Scenario Outline: Check pgconsul-util targeted switchover works as expected
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
                config:
                    pgconsul.conf:
                        global:
                            priority: 1
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
                            priority: 3
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql3" is in <replication_type> group
        When we run following command on host "postgresql1"
        """
        pgconsul-util switchover --yes --block --destination pgconsul_postgresql2_1.pgconsul_pgconsul_net
        """
        Then command exit with return code "0"
        And command result contains following output
        """
        switchover finished, zk status "None"
        """
        Then container "postgresql2" became a primary
        And container "postgresql1" is a replica of container "postgresql2"
        And container "postgresql3" is a replica of container "postgresql2"
        And container "postgresql3" is in <replication_type> group
        And postgresql in container "postgresql1" was not rewinded
        And postgresql in container "postgresql3" was not rewinded
    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  | replication_type | quorum_commit |
        | zookeeper | zookeeper1 |      sync        |       no      |
        | zookeeper | zookeeper1 |      quorum      |      yes      |


    @pgconsul_util_switchover_reset
    Scenario Outline: Check pgconsul-util switchover reset works as expected
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
                            priority: 1
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
                            priority: 3
        """
        When we gracefully stop "pgconsul" in container "postgresql1"
        And we gracefully stop "pgconsul" in container "postgresql2"
        And we gracefully stop "pgconsul" in container "postgresql3"
        And we lock "/pgconsul/postgresql/alive/pgconsul_postgresql1_1.pgconsul_pgconsul_net" in <lock_type> "<lock_host>"
        And we lock "/pgconsul/postgresql/alive/pgconsul_postgresql2_1.pgconsul_pgconsul_net" in <lock_type> "<lock_host>"
        And we lock "/pgconsul/postgresql/alive/pgconsul_postgresql3_1.pgconsul_pgconsul_net" in <lock_type> "<lock_host>"
        And we lock "/pgconsul/postgresql/leader" in <lock_type> "<lock_host>" with value "pgconsul_postgresql1_1.pgconsul_pgconsul_net"
        And we run following command on host "postgresql1"
        """
        pgconsul-util switchover --yes
        """
        Then command exit with return code "0"
        And command result contains following output
        """
        scheduled
        """
        Then <lock_type> "<lock_host>" has value "scheduled" for key "/pgconsul/postgresql/switchover/state"
        And <lock_type> "<lock_host>" has value "{"hostname": "pgconsul_postgresql1_1.pgconsul_pgconsul_net", "timeline": 1, "destination": null}" for key "/pgconsul/postgresql/switchover/master"
        When we run following command on host "postgresql1"
        """
        pgconsul-util switchover --reset
        """
        Then command exit with return code "0"
        And command result contains following output
        """
        resetting ZK switchover nodes
        """
        Then <lock_type> "<lock_host>" has value "failed" for key "/pgconsul/postgresql/switchover/state"
        And <lock_type> "<lock_host>" has value "{}" for key "/pgconsul/postgresql/switchover/master"
        When we run following command on host "postgresql1"
        """
        pgconsul-util switchover --yes
        """
        Then command exit with return code "0"
        And command result contains following output
        """
        scheduled
        """
        Then <lock_type> "<lock_host>" has value "scheduled" for key "/pgconsul/postgresql/switchover/state"
        And <lock_type> "<lock_host>" has value "{"hostname": "pgconsul_postgresql1_1.pgconsul_pgconsul_net", "timeline": 1, "destination": null}" for key "/pgconsul/postgresql/switchover/master"
        When we release lock "/pgconsul/postgresql/alive/pgconsul_postgresql1_1.pgconsul_pgconsul_net" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/alive/pgconsul_postgresql2_1.pgconsul_pgconsul_net" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/alive/pgconsul_postgresql3_1.pgconsul_pgconsul_net" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/leader" in <lock_type> "<lock_host>" with value "pgconsul_postgresql1_1.pgconsul_pgconsul_net"
        And we start "pgconsul" in container "postgresql1"
        And we start "pgconsul" in container "postgresql2"
        And we start "pgconsul" in container "postgresql3"
        Then <lock_type> "<lock_host>" has no value for key "/pgconsul/postgresql/switchover/state"
        And <lock_type> "<lock_host>" has no value for key "/pgconsul/postgresql/switchover/master"
    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |

    @pgconsul_util_initzk @pgconsul_util_initzk_test
    Scenario Outline: Check pgconsul-util initzk --test works as expected
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
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" with replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
        """
        When we gracefully stop "pgconsul" in container "postgresql1"
        And we gracefully stop "pgconsul" in container "postgresql2"
        And we remove key "/pgconsul/postgresql" in <lock_type> "<lock_host>"
        And we run following command on host "postgresql1"
        """
        pgconsul-util initzk --test pgconsul_postgresql1_1.pgconsul_pgconsul_net pgconsul_postgresql2_1.pgconsul_pgconsul_net
        """
        Then command exit with return code "2"
        And command result contains following output
        """
        Path "all_hosts/pgconsul_postgresql1_1.pgconsul_pgconsul_net" not found in ZK, initialization has not been performed earlier
        """

        When we start "pgconsul" in container "postgresql1"
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And <lock_type> "<lock_host>" has value "0" for key "/pgconsul/postgresql/all_hosts/pgconsul_postgresql1_1.pgconsul_pgconsul_net/prio"

        When we run following command on host "postgresql1"
        """
        pgconsul-util initzk --test pgconsul_postgresql1_1.pgconsul_pgconsul_net pgconsul_postgresql2_1.pgconsul_pgconsul_net
        """
        Then command exit with return code "2"
        And command result contains following output
        """
        Path "all_hosts/pgconsul_postgresql2_1.pgconsul_pgconsul_net" not found in ZK, initialization has not been performed earlier
        """

        When we start "pgconsul" in container "postgresql2"
        Then container "postgresql2" is in quorum group

        When we run following command on host "postgresql1"
        """
        pgconsul-util initzk --test pgconsul_postgresql1_1.pgconsul_pgconsul_net pgconsul_postgresql2_1.pgconsul_pgconsul_net
        """
        Then command exit with return code "0"
        And command result contains following output
        """
        Initialization for all fqdns has been performed earlier
        """
    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |

    @pgconsul_util_initzk @pgconsul_util_initzk_do_init
    Scenario Outline: Check pgconsul-util initzk works as expected
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
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" with replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql2" is in quorum group
        When we run following command on host "postgresql1"
        """
        pgconsul-util switchover --yes --block
        """
        Then command exit with return code "0"

        When we gracefully stop "pgconsul" in container "postgresql1"
        And we gracefully stop "pgconsul" in container "postgresql2"
        And we remove key "/pgconsul/postgresql" in <lock_type> "<lock_host>"
        And we start "pgconsul" in container "postgresql1"
        And we start "pgconsul" in container "postgresql2"
        And we wait "10.0" seconds
        Then "pgconsul" is not running in container "postgresql1"
        And "pgconsul" is not running in container "postgresql2"

        When we run following command on host "postgresql1"
        """
        pgconsul-util initzk pgconsul_postgresql1_1.pgconsul_pgconsul_net pgconsul_postgresql2_1.pgconsul_pgconsul_net
        """
        Then command exit with return code "0"
        And command result contains following output
        """
        ZK structures are initialized
        """

        When we start "pgconsul" in container "postgresql1"
        And we start "pgconsul" in container "postgresql2"
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql2_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql1" is in quorum group
    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |

    @pgconsul_util_initzk @pgconsul_util_initzk_errors_handling
    Scenario Outline: Check pgconsul-util initzk works as expected
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
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" with replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
        """
        When we disconnect from network container "zookeeper1"
        And we disconnect from network container "zookeeper2"
        And we disconnect from network container "zookeeper3"
        And we run following command on host "postgresql1"
        """
        pgconsul-util initzk --test pgconsul_postgresql1_1.pgconsul_pgconsul_net pgconsul_postgresql2_1.pgconsul_pgconsul_net
        """
        Then command exit with return code "1"
        And command result contains following output
        """
        KazooTimeoutError
        """

        When we run following command on host "postgresql1"
        """
        pgconsul-util initzk pgconsul_postgresql1_1.pgconsul_pgconsul_net pgconsul_postgresql2_1.pgconsul_pgconsul_net
        """
        Then command exit with return code "1"
        And command result contains following output
        """
        Could not create path "all_hosts/pgconsul_postgresql1_1.pgconsul_pgconsul_net" in ZK
        """
    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |


    @pgconsul_util_info
    Scenario Outline: Check pgconsul-util info for single-host cluster.
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
                    sync_replication_in_maintenance: 'no'
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        Given a following cluster with "<lock_type>" with replication slots
        """
            postgresql1:
                role: primary
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        When we run following command on host "postgresql1"
        """
        pgconsul-util info
        """
        Then command exit with return code "0"
        And command result contains following output
        """
        alive: true
        """
        When we run following command on host "postgresql1"
        """
        pgconsul-util info -s --json
        """
        Then command exit with return code "0"
        And command result contains following output
        """
        {
            "alive": true,
            "last_failover_time": null,
            "maintenance": null,
            "primary": "pgconsul_postgresql1_1.pgconsul_pgconsul_net",
            "replics_info": {}
        }
        """
    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |


    @pgconsul_util_info
    Scenario Outline: Check pgconsul-util info for HA cluster works
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
                            priority: 2
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
                            priority: 3
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        When we run following command on host "postgresql1"
        """
        pgconsul-util info
        """
        Then command exit with return code "0"
        When we run following command on host "postgresql2"
        """
        pgconsul-util info -js
        """
        Then command exit with return code "0"
    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |


  Scenario Outline: Check pgconsul-util info with cascade replica
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'no'
                    postgres_timeout: 5
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
                            stream_from: pgconsul_postgresql2_1.pgconsul_pgconsul_net
                stream_from: postgresql2
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        When we run following command on host "postgresql1"
        """
        pgconsul-util info
        """
        Then command exit with return code "0"
        When we run following command on host "postgresql2"
        """
        pgconsul-util info -js
        """
        Then command exit with return code "0"

    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |
