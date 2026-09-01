Feature: Operator-initiated failover
    Background:
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    quorum_commit: 'yes'
                    autofailover: 'no'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                replica:
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 1
                    min_failover_timeout: 120
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
        """
        And a following cluster with "zookeeper" with replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 200
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 100
        """
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql2" is in quorum group
        And container "postgresql3" is in quorum group
        And container "postgresql2" is streaming from container "postgresql1"
        And container "postgresql3" is streaming from container "postgresql1"

    @manual_failover_safe
    Scenario: Ordinary manual failover uses normal safety checks
        When we run following command on host "postgresql2"
        """
        pgconsul-util failover
        """
        Then command exit with return code "0"
        And command result contains following output
        """
        requested failover of pgconsul_postgresql1_1.pgconsul_pgconsul_net
        """
        Then we remember which of "postgresql2,postgresql3" became primary as "new_primary" and the other as "new_replica"
        And container "new_replica" is streaming from container "new_primary"

    @manual_failover_data_loss
    Scenario: Data-loss mode proceeds with an incomplete durability read quorum
        When we disconnect from network container "postgresql3"
        When we run following command on host "postgresql2"
        """
        pgconsul-util failover --with-data-loss --yes --timeout 3
        """
        Then command exit with return code "0"
        And command result contains following output
        """
        pgconsul_postgresql2_1.pgconsul_pgconsul_net: UNSAFE
        """
        And command result contains following output
        """
        not enough votes
        """
        Then container "postgresql2" became a primary

    @manual_failover_specific_host
    Scenario: Data-loss mode promotes the explicitly selected voted host
        When we run following command on host "postgresql2"
        """
        bash -c "printf '%s\n' 'pgconsul_postgresql3_1.pgconsul_pgconsul_net' | pgconsul-util failover --with-data-loss --timeout 10"
        """
        Then command exit with return code "0"
        And command result contains following output
        """
        selected failover winner pgconsul_postgresql3_1.pgconsul_pgconsul_net
        """
        Then container "postgresql3" became a primary

    @manual_failover_force_lock
    Scenario: Coordinator removes a stale old-primary leader lock
        When we gracefully stop "pgconsul" in container "postgresql1"
        And we lock "/pgconsul/postgresql/leader" in zookeeper "zookeeper1" with value "pgconsul_postgresql1_1.pgconsul_pgconsul_net"
        Then zookeeper "zookeeper1" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        When we run following command on host "postgresql2"
        """
        pgconsul-util failover
        """
        Then command exit with return code "0"
        And one of containers "postgresql2,postgresql3" pgconsul log contains "Forcing stale primary pgconsul_postgresql1_1.pgconsul_pgconsul_net to release the leader lock"
        And within "30" seconds zookeeper "zookeeper1" has one of holders "pgconsul_postgresql2_1.pgconsul_pgconsul_net,pgconsul_postgresql3_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        When we start "pgconsul" in container "postgresql1"
        Then we remember which of "postgresql2,postgresql3" became primary as "new_primary" and the other as "new_replica"

    @manual_failover_no_wal_fencing
    Scenario: Data-loss mode can explicitly leave WAL sources unfenced
        When we disconnect from network container "postgresql3"
        And we run following command on host "postgresql2"
        """
        pgconsul-util failover --with-data-loss --no-wal-fencing --yes --timeout 3
        """
        Then command exit with return code "0"
        And command result contains following output
        """
        WARNING: restore_command and walreceiver were not disabled; vote positions are not frozen.
        """
        And command result contains following output
        """
        pgconsul_postgresql2_1.pgconsul_pgconsul_net: UNSAFE
        """
        And one of containers "postgresql2,postgresql3" pgconsul log contains "Collecting an unfenced failover vote"
        Then container "postgresql2" became a primary
