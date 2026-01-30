Feature: Check maintenance mode

    @maintenance_exit
    Scenario Outline: Single-host cluster should exit from the maintenance mode when Postgres is dead.
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
        When we set value "enable" for key "/pgconsul/postgresql/maintenance" in <lock_type> "<lock_host>"
        And we wait "10.0" seconds
        And we gracefully stop "postgres" in container "postgresql1"
        When we set value "disable" for key "/pgconsul/postgresql/maintenance" in <lock_type> "<lock_host>"
        And we wait "10.0" seconds
        Then <lock_type> "<lock_host>" has value "None" for key "/pgconsul/postgresql/maintenance"
        And container "postgresql1" became a primary
    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |



    @maintenance_exit
    Scenario Outline: Single-host cluster should exit from the maintenance mode when the container is unavailable.
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
        When we set value "enable" for key "/pgconsul/postgresql/maintenance" in <lock_type> "<lock_host>"
        And we wait "10.0" seconds
        When we <destroy> container "postgresql1"
        And we wait "10.0" seconds
        When we <repair> container "postgresql1"
        And we wait "10.0" seconds
        When we set value "disable" for key "/pgconsul/postgresql/maintenance" in <lock_type> "<lock_host>"
        And we wait "10.0" seconds
        Then <lock_type> "<lock_host>" has value "None" for key "/pgconsul/postgresql/maintenance"
        And container "postgresql1" became a primary
    Examples: <lock_type>, <lock_host>, <destroy>, <repair>
        | lock_type | lock_host  |          destroy        |       repair       |
        | zookeeper | zookeeper1 |           stop          |        start       |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |
        | zookeeper | zookeeper1 |           stop          |        start       |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |



    Scenario Outline: Check container stop in maintenance mode
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
                            priority: 2
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 1
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in <replication_type> group
        And container "postgresql2" is a replica of container "postgresql1"
        And container "postgresql3" is a replica of container "postgresql1"
        When we set value "enable" for key "/pgconsul/postgresql/maintenance" in <lock_type> "<lock_host>"
        And we wait "10.0" seconds
        And we stop container "postgresql1"
        And we wait "10.0" seconds
        And we start container "postgresql1"
        And we start "postgres" in container "postgresql1"
        And we wait "10.0" seconds
        When we set value "disable" for key "/pgconsul/postgresql/maintenance" in <lock_type> "<lock_host>"
        And we wait "10.0" seconds
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql1" became a primary
        Then container "postgresql2" is in <replication_type> group
        And container "postgresql2" is a replica of container "postgresql1"
        And container "postgresql3" is a replica of container "postgresql1"
    Examples: <lock_type>, <lock_host>, <quorum_commit>, <replication_type>
        | lock_type | lock_host  | quorum_commit | replication_type |
        | zookeeper | zookeeper1 |      yes      |      quorum      |
        | zookeeper | zookeeper1 |      no       |       sync       |



    Scenario Outline: Check pgbouncer is untouchable in maintenance mode
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
                            priority: 2
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 1
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql2" is a replica of container "postgresql1"
        And container "postgresql3" is a replica of container "postgresql1"
        When we set value "enable" for key "/pgconsul/postgresql/maintenance" in <lock_type> "<lock_host>"
        And we wait "10.0" seconds
        Then pgbouncer is running in container "postgresql1"
        When we disconnect from network container "postgresql1"
        And we wait "10.0" seconds
        Then pgbouncer is running in container "postgresql1"
        When we connect to network container "postgresql1"
        And we wait "10.0" seconds
        Then pgbouncer is running in container "postgresql1"
    Examples: <lock_type>, <lock_host>, <quorum_commit>
        | lock_type | lock_host  | quorum_commit |
        | zookeeper | zookeeper1 |      yes      |
        | zookeeper | zookeeper1 |      no       |



    Scenario Outline: Sync replication turns off in maintenance
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
                    sync_replication_in_maintenance: 'no'
                replica:
                    allow_potential_data_loss: 'no'
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
                            priority: 2
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 1
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in <replication_type> group
        And container "postgresql2" is a replica of container "postgresql1"
        And container "postgresql3" is a replica of container "postgresql1"
        When we set value "enable" for key "/pgconsul/postgresql/maintenance" in <lock_type> "<lock_host>"
        And we wait "10.0" seconds
        Then container "postgresql1" replication state is "async"
        And  postgresql in container "postgresql1" has empty option "synchronous_standby_names"
        When we set value "disable" for key "/pgconsul/postgresql/maintenance" in <lock_type> "<lock_host>"
        And we wait "10.0" seconds
        Then container "postgresql2" is in <replication_type> group
    Examples: <lock_type>, <lock_host>, <quorum_commit>, <replication_type>
        | lock_type | lock_host  | quorum_commit | replication_type |
        | zookeeper | zookeeper1 |      yes      |      quorum      |
        | zookeeper | zookeeper1 |      no       |       sync       |


	@maintenance_primary
    Scenario Outline: Node with current primary exists in maintenance
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
                replica:
                    allow_potential_data_loss: 'no'
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
                            priority: 2
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 1
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql2" is a replica of container "postgresql1"
        And container "postgresql3" is a replica of container "postgresql1"
        And container "postgresql2" is in quorum group
        And container "postgresql3" is in quorum group
        When we set value "enable" for key "/pgconsul/postgresql/maintenance" in <lock_type> "<lock_host>"
        And we wait "10.0" seconds
        Then <lock_type> "<lock_host>" has value "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for key "/pgconsul/postgresql/maintenance/master"
        When we set value "disable" for key "/pgconsul/postgresql/maintenance" in <lock_type> "<lock_host>"
        And we wait "10.0" seconds
        Then <lock_type> "<lock_host>" has no value for key "/pgconsul/postgresql/maintenance/master"
    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |

    Scenario Outline: No splitbrain in maintenance mode after failover
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
                replica:
                    allow_potential_data_loss: 'no'
                    primary_switch_checks: 1
                    min_failover_timeout: 1
                    primary_unavailability_timeout: 2
                    primary_switch_restart: no
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
                            priority: 2
            postgresql3:
                role: replica
                config:
                    pgconsul.conf:
                        global:
                            priority: 1
        """
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        Then container "postgresql2" is in quorum group
        Then container "postgresql3" is in quorum group
        Then <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: quorum
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
            sync_state: quorum
        """
        When we gracefully stop "pgconsul" in container "postgresql1"
        When we disconnect from network container "postgresql1"
        Then one of the containers "postgresql2,postgresql3" became a primary, and we remember it
        Then <lock_type> "<lock_host>" has value "2" for key "/pgconsul/postgresql/timeline"
        When we set value "enable" for key "/pgconsul/postgresql/maintenance" in <lock_type> "<lock_host>"
        And we connect to network container "postgresql1"
        And we start "pgconsul" in container "postgresql1"
        Then pgbouncer is not running in container "postgresql1"
        And pgbouncer is running in remembered container
        When we wait "10.0" seconds
        Then container "postgresql1" replication state is "sync"
        And pgbouncer is not running in container "postgresql1"
        And pgbouncer is running in remembered container
        When we set value "disable" for key "/pgconsul/postgresql/maintenance" in <lock_type> "<lock_host>"
        Then another of the containers "postgresql2,postgresql3" is a replica
        And container "postgresql1" is a replica of remembered container
        And container "postgresql1" is in quorum group
        And pgbouncer is running in container "postgresql1"

    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  |
        | zookeeper | zookeeper1 |
