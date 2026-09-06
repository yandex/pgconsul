Feature: Return a host to the cluster

    # These scenarios exercise the persistent return-to-cluster machine as a
    # single host-local workflow. Election and switchover features cover their
    # own protocols; this feature pins the recovery contracts after they select
    # a primary.

    @return_to_cluster @return_from_primary
    Scenario: Stopped replica starts from its unchanged primary
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'no'
                    quorum_commit: 'yes'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                replica:
                    recovery_timeout: 10
                    primary_switch_checks: 1
                    primary_switch_restart: 'no'
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_without_slot.sh %m %p
        """
        And a following cluster with "zookeeper" without replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
            postgresql3:
                role: replica
        """
        Then container "postgresql2" is streaming from container "postgresql1"
        When we gracefully stop "postgres" in container "postgresql2"
        Then container "postgresql2" is a replica of container "postgresql1" and streaming
        And postgresql in container "postgresql2" was not rewinded

    @return_to_cluster @return_after_rewind
    Scenario: Former primary rewinds and waits for its asynchronous start
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'no'
                    quorum_commit: 'yes'
                    max_rewind_retries: 1
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                replica:
                    primary_unavailability_timeout: 2
                    primary_switch_checks: 1
                    min_failover_timeout: 1
                    recovery_timeout: 10
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_without_slot.sh %m %p
                    pg_start: sleep 3 && /usr/local/bin/cleanup_stale_postmaster_pid.sh %p && /usr/bin/postgresql/pg_ctl start -s -w -t %t -D %p --log=/var/log/postgresql/postgresql.log
        """
        And a following cluster with "zookeeper" without replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
            postgresql3:
                role: replica
        """
        When we stop container "postgresql1"
        Then we remember which of "postgresql2,postgresql3" became primary as "new_primary" and the other as "new_replica"
        When we start container "postgresql1"
        Then container "postgresql1" is a replica of container "new_primary" and streaming
        And postgresql in container "postgresql1" was rewinded

    @return_to_cluster @return_archive_fallback
    Scenario: Losing replica falls back to archive after target remaster stalls
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    election_timeout: 20
                    autofailover: 'yes'
                    quorum_commit: 'yes'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                replica:
                    primary_unavailability_timeout: 2
                    primary_switch_checks: 1
                    min_failover_timeout: 1
                    recovery_timeout: 5
                    primary_switch_restart: 'no'
                    return_lsn_stall_timeout: 2
            postgresql.conf:
                synchronous_commit: 'on'
        """
        And a following cluster with "zookeeper" without replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 3
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 2
            postgresql4:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 1
        """
        Then container "postgresql2" is in quorum group
        And container "postgresql3" is in quorum group
        And container "postgresql4" is in quorum group
        # The replica can vote, but cannot stream from either possible winner or fetch S3.
        When we block postgres traffic from "postgresql2" to "postgresql4"
        And we block postgres traffic from "postgresql3" to "postgresql4"
        And we run following command on host "postgresql4"
        """
        sh -c "iptables -I OUTPUT -p tcp --dport 873 -j REJECT"
        """
        When we stop container "postgresql1"
        Then we remember which of "postgresql2,postgresql3" became primary as "new_primary" and the other as "new_replica"
        Then container "postgresql4" pgconsul log contains messages in order within "60" seconds
        """
        Primary remaster made no receive progress; falling back to archive recovery
        Waiting for timeline 2 history in the archive
        """
        When we run following command on host "postgresql2"
        """
        sh -c "iptables -F"
        """
        And we run following command on host "postgresql3"
        """
        sh -c "iptables -F"
        """
        And we run following command on host "postgresql4"
        """
        sh -c "iptables -F"
        """
        Then container "postgresql4" is a replica of container "new_primary" and streaming

    @return_to_cluster @return_resetup_required
    Scenario: Manual resetup release retries a failed rewind
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'no'
                    quorum_commit: 'yes'
                    max_rewind_retries: 1
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                replica:
                    primary_unavailability_timeout: 2
                    primary_switch_checks: 1
                    min_failover_timeout: 1
                    recovery_timeout: 10
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_without_slot.sh %m %p
                    rewind: test -f /tmp/allow_rewind && /usr/local/bin/cleanup_stale_postmaster_pid.sh %p && /usr/bin/postgresql/pg_rewind --restore-target-wal --target-pgdata=%p --source-server='host=%m dbname=postgres user=postgres connect_timeout=1'
        """
        And a following cluster with "zookeeper" without replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
            postgresql3:
                role: replica
        """
        When we stop container "postgresql1"
        Then we remember which of "postgresql2,postgresql3" became primary as "new_primary" and the other as "new_replica"
        When we start container "postgresql1"
        Then container "postgresql1" pgconsul log contains "Return to cluster cannot make progress; resetup is required"
        # Let the terminal transition finish before the external resetup actor
        # removes its release flag.
        When we wait "2" seconds
        When we run following command on host "postgresql1"
        """
        test -f /tmp/.pgconsul_rewind_fail.flag
        """
        Then command exit with return code "0"
        When we run following command on host "postgresql1"
        """
        touch /tmp/allow_rewind
        """
        Then command exit with return code "0"
        When we run following command on host "postgresql1"
        """
        rm -f /tmp/.pgconsul_rewind_fail.flag
        """
        Then command exit with return code "0"
        When we run following command on host "postgresql1"
        """
        test ! -f /tmp/.pgconsul_rewind_fail.flag
        """
        Then command exit with return code "0"
        Then container "postgresql1" is a replica of container "new_primary" and streaming

    @return_to_cluster @return_after_fork_rewind
    Scenario: Replica past the failover fork point rewinds
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'no'
                    quorum_commit: 'yes'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                    manual_durability_exclusion_timeout: 86400
                replica:
                    primary_unavailability_timeout: 2
                    primary_switch_checks: 1
                    min_failover_timeout: 1
                    recovery_timeout: 10
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_without_slot.sh %m %p
        """
        And a following cluster with "zookeeper" without replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 3
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 2
            postgresql4:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 1
        """
        Then container "postgresql2" is in quorum group
        And container "postgresql3" is in quorum group
        And container "postgresql4" is in quorum group
        When we run following command on host "postgresql1"
        """
        pgconsul-util durability-exclude pgconsul_postgresql4_1.pgconsul_pgconsul_net --wait 30
        """
        Then command exit with return code "0"
        And container "postgresql4" is not in quorum group
        When we run following command on host "postgresql1"
        """
        su - postgres -c "psql -d postgres -c 'CREATE TABLE return_after_fork (id integer)'"
        """
        Then command exit with return code "0"
        And we remove rewind flag in container "postgresql4"
        When we disconnect from ZK container "postgresql4"
        And we disconnect from ZK container "postgresql1"
        And we block postgres traffic from "postgresql1" to "postgresql2"
        And we block postgres traffic from "postgresql1" to "postgresql3"
        Then we remember which of "postgresql2,postgresql3" became primary as "new_primary" and the other as "new_replica"
        When we run following command on host "postgresql1"
        """
        su - postgres -c "psql -d postgres -c 'SET synchronous_commit = off; INSERT INTO return_after_fork SELECT generate_series(1, 1000); SELECT pg_switch_wal();'"
        """
        Then command exit with return code "0"
        And container "postgresql4" has "1000" rows in table "return_after_fork"
        When we connect to ZK container "postgresql4"
        Then container "postgresql4" is a replica of container "new_primary"
        And container "postgresql4" is streaming from container "new_primary"
        And postgresql in container "postgresql4" was rewinded
