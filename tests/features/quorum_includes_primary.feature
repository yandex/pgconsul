Feature: Quorum size in synchronous_standby_names

    # `quorum_includes_primary` counts the primary's own copy of a commit towards
    # the quorum, so a commit is confirmed by a majority of the whole cluster.
    # With `no` the quorum is a majority of the replicas alone: a commit survives
    # the loss of the primary and one more host, which costs one extra replica in
    # `ANY N(...)` on clusters with an odd number of hosts.

    Scenario Outline: Quorum size with default (not configured) quorum_includes_primary
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
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 1
                    min_failover_timeout: 1
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_without_slot.sh %m %p
        """
        Given a following cluster of "<hosts>" hosts with "zookeeper" without replication slots
        Then synchronous_standby_names in container "postgresql1" requires "<quorum>" of "<replicas>" replicas

    Examples: default is to count the primary in the quorum
        | hosts | replicas | quorum |
        | 5     | 4        | 2      |
        | 4     | 3        | 2      |
        | 3     | 2        | 1      |
        | 2     | 1        | 1      |
        | 1     | 0        | 0      |

    Scenario Outline: Quorum size with explicit quorum_includes_primary
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'no'
                    quorum_commit: 'yes'
                    quorum_includes_primary: '<includes_primary>'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 1
                    min_failover_timeout: 1
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_without_slot.sh %m %p
        """
        Given a following cluster of "<hosts>" hosts with "zookeeper" without replication slots
        Then synchronous_standby_names in container "postgresql1" requires "<quorum>" of "<replicas>" replicas

    Examples: quorum of the whole cluster, the primary included
        | includes_primary | hosts | replicas | quorum |
        | yes              | 5     | 4        | 2      |
        | yes              | 4     | 3        | 2      |
        | yes              | 3     | 2        | 1      |
        | yes              | 2     | 1        | 1      |
        | yes              | 1     | 0        | 0      |

    Examples: quorum of the replicas alone, one more replica required for quorum on odd-sized clusters
        | includes_primary | hosts | replicas | quorum |
        | no               | 5     | 4        | 3      |
        | no               | 4     | 3        | 2      |
        | no               | 3     | 2        | 2      |
        | no               | 2     | 1        | 1      |
        | no               | 1     | 0        | 0      |

    # A quorum of the replicas alone puts every commit on both replicas of a three
    # host cluster, so the last host standing has all of them and may be promoted.
    @failover
    Scenario: The last host of a three host cluster is promoted
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'no'
                    quorum_commit: 'yes'
                    quorum_includes_primary: 'no'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 1
                    min_failover_timeout: 1
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_without_slot.sh %m %p
        """
        Given a following cluster of "3" hosts with "zookeeper" without replication slots
        Then container "postgresql2" is in quorum group
        And container "postgresql3" is in quorum group
        And synchronous_standby_names in container "postgresql1" requires "2" of "2" replicas

        # the primary and one of its replicas fail at once, ZooKeeper stays available
        When we disconnect from network container "postgresql1"
        And we disconnect from network container "postgresql2"
        Then container "postgresql3" became a primary

    # The same failure while the primary still counted itself in the quorum: a commit
    # could be on the lost replica only, so the last host must refuse to promote --
    # even though its own option says the quorum excludes the primary, as it does
    # while the option is being rolled out over a cluster.
    @failover
    Scenario: The last host refuses to promote after the primary counted itself
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'no'
                    quorum_commit: 'yes'
                    quorum_includes_primary: 'no'
                primary:
                    change_replication_type: 'yes'
                    primary_switch_checks: 1
                replica:
                    allow_potential_data_loss: 'no'
                    primary_unavailability_timeout: 1
                    primary_switch_checks: 1
                    min_failover_timeout: 1
                commands:
                    generate_recovery_conf: /usr/local/bin/gen_rec_conf_without_slot.sh %m %p
        """
        Given a following cluster with "zookeeper" without replication slots
        """
            postgresql1:
                role: primary
                config:
                    pgconsul.conf:
                        global:
                            quorum_includes_primary: 'yes'
            postgresql2:
                role: replica
            postgresql3:
                role: replica
        """
        Then container "postgresql2" is in quorum group
        And container "postgresql3" is in quorum group
        And synchronous_standby_names in container "postgresql1" requires "1" of "2" replicas

        # the primary and one of its replicas fail at once, ZooKeeper stays available
        When we disconnect from network container "postgresql1"
        And we disconnect from network container "postgresql2"
        Then container "postgresql3" pgconsul log contains messages in order within "60" seconds
        """
        Promote is not allowed with given configuration.
        """
        When we wait "10.0" seconds
        Then zookeeper "zookeeper1" has holder "None" for lock "/pgconsul/postgresql/leader"
