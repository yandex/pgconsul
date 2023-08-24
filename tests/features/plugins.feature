Feature: Check plugins

    @plugins
    Scenario Outline: Check upload_wals plugin
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'yes'
                    use_lwaldump: 'no'
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
                            priority: 1
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 2
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql3_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/sync_replica"
        When we disable archiving in "postgresql1"
        And we switch wal in "postgresql1" "10" times
        And we <destroy> container "postgresql1"
        Then container "postgresql3" became a primary
        And wals present on backup "<backup_host>"
    Examples: <lock_type>, <backup_host>, <lock_host>, <destroy>
        | lock_type | backup_host  | lock_host  | destroy                 |
        | zookeeper | backup1      | zookeeper1 | stop                    |
        | zookeeper | backup1      | zookeeper1 | disconnect from network |
