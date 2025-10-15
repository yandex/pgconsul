Feature: Check pgconsul with disabled autofailover
    @switchover_test
    Scenario Outline: Check switchover with disabled autofailover
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
                    min_failover_timeout: 60
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
        Then container "postgresql3" is in <replication_type> group
        When we lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        And we set value "{'hostname': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net','timeline': 1}" for key "/pgconsul/postgresql/switchover/master" in <lock_type> "<lock_host>"
        And we set value "scheduled" for key "/pgconsul/postgresql/switchover/state" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        Then container "postgresql3" became a primary
        And container "postgresql2" is a replica of container "postgresql3"
        And container "postgresql1" is a replica of container "postgresql3"
        Then postgresql in container "postgresql2" was not rewinded
        Then postgresql in container "postgresql1" was rewinded
        Then container "postgresql1" is in <replication_type> group
        When we lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        And we set value "{'hostname': 'pgconsul_postgresql3_1.pgconsul_pgconsul_net','timeline': 2}" for key "/pgconsul/postgresql/switchover/master" in <lock_type> "<lock_host>"
        And we set value "scheduled" for key "/pgconsul/postgresql/switchover/state" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        Then container "postgresql1" became a primary
        And container "postgresql3" is a replica of container "postgresql1"
        And container "postgresql2" is a replica of container "postgresql1"
        When we stop container "postgresql2"
        And we lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        And we set value "{'hostname': 'pgconsul_postgresql1_1.pgconsul_pgconsul_net','timeline': 3}" for key "/pgconsul/postgresql/switchover/master" in <lock_type> "<lock_host>"
        And we set value "scheduled" for key "/pgconsul/postgresql/switchover/state" in <lock_type> "<lock_host>"
        And we release lock "/pgconsul/postgresql/switchover/lock" in <lock_type> "<lock_host>"
        And we wait "30.0" seconds
        Then container "postgresql1" is primary
        When we wait "30.0" seconds
        Then container "postgresql3" became a primary
        And container "postgresql1" is a replica of container "postgresql3"

    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  | quorum_commit | replication_type |
        | zookeeper | zookeeper1 |      yes      |      quorum      |
        | zookeeper | zookeeper1 |      no       |       sync       |

    @failover
    Scenario Outline: Check kill primary with disabled autofailover
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
        And <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
        """
        When we <destroy> container "postgresql1"
        And we wait "30.0" seconds
        Then <lock_type> "<lock_host>" has holder "None" for lock "/pgconsul/postgresql/leader"
        When we <repair> container "postgresql1"
        Then container "postgresql2" is a replica of container "postgresql1"
        And container "postgresql3" is a replica of container "postgresql1"

    Examples: <lock_type>, <lock_host>, <destroy>, <repair>
        | lock_type | lock_host  |          destroy        |       repair       | quorum_commit |
        | zookeeper | zookeeper1 |           stop          |        start       |      yes      |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |      yes      |
        | zookeeper | zookeeper1 |           stop          |        start       |      no       |
        | zookeeper | zookeeper1 | disconnect from network | connect to network |      no       |

    Scenario Outline: Check suddenly external promote replica
    We consider unexpected external promote as an error, so we leave old primary as it is.
    Moreover, pgconsul should switch off pgbouncer on suddenly promoted host to avoid split brain state.
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
        And <lock_type> "<lock_host>" has following values for key "/pgconsul/postgresql/replics_info"
        """
          - client_hostname: pgconsul_postgresql3_1.pgconsul_pgconsul_net
            state: streaming
          - client_hostname: pgconsul_postgresql2_1.pgconsul_pgconsul_net
            state: streaming
        """
        When we promote host "postgresql2"
        Then <lock_type> "<lock_host>" has holder "pgconsul_postgresql1_1.pgconsul_pgconsul_net" for lock "/pgconsul/postgresql/leader"
        And container "postgresql1" is primary
        And pgbouncer is not running in container "postgresql2"
        And pgbouncer is running in container "postgresql1"
        And pgbouncer is running in container "postgresql3"

    Examples: <lock_type>, <lock_host>
        | lock_type | lock_host  | quorum_commit |
        | zookeeper | zookeeper1 |      yes      |
        | zookeeper | zookeeper1 |      no       |
