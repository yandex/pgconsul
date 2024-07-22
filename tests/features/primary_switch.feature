Feature: Check primary switch logic

    @switchover
    Scenario Outline: Correct primary switch from shut down after switchover
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
                    min_failover_timeout: 120
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
        Then container "postgresql3" is in <replication_type> group
        And container "postgresql2" is a replica of container "postgresql1"
        When we gracefully stop "pgconsul" in container "postgresql2"
        And we kill "postgres" in container "postgresql2" with signal "9"
        And we lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        And we set value "{'hostname': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net','timeline': 1}" for key "/pgconsul/postgresql/switchover/master" in <lock_type> "<lock_host>"
        And we set value "scheduled" for key "/pgconsul/postgresql/switchover/state" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        Then container "postgresql3" became a primary
        And container "postgresql1" is a replica of container "postgresql3"
        And postgresql in container "postgresql1" was not rewinded
        When we start "pgconsul" in container "postgresql2"
        Then <lock_type> "<lock_host>" has value "yes" for key "/pgconsul/postgresql/all_hosts/pgconsul_postgresql2_1.pgconsul_pgconsul_net/tried_remaster"
        And container "postgresql2" is a replica of container "postgresql3"
        And postgresql in container "postgresql2" was not rewinded

    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  | quorum_commit | replication_type |
        | zookeeper | zookeeper1 |      yes      |      quorum      |
        | zookeeper | zookeeper1 |      no       |       sync       |
