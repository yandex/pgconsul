Feature: Quorum size in synchronous_standby_names

    # `forced_quorum_replicas_number` sets how many replicas have to confirm a commit
    # instead of the majority pgconsul computes on its own. It never exceeds the group
    # the confirmations are required from, so commits keep flowing on a cluster smaller
    # than the setting and on one that has lost a replica.

    Scenario Outline: Quorum size with default (not configured) forced_quorum_replicas_number
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

    Examples: a majority of the cluster, the primary counted in
        | hosts | replicas | quorum |
        | 5     | 4        | 2      |
        | 4     | 3        | 2      |
        | 3     | 2        | 1      |
        | 2     | 1        | 1      |
        | 1     | 0        | 0      |

    Scenario Outline: Quorum size with forced_quorum_replicas_number set
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'no'
                    quorum_commit: 'yes'
                    forced_quorum_replicas_number: '<forced>'
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

    Examples: two replicas confirm every commit, whatever the cluster size
        | forced | hosts | replicas | quorum |
        | 2      | 5     | 4        | 2      |
        | 2      | 4     | 3        | 2      |
        | 2      | 3     | 2        | 2      |
        | 2      | 2     | 1        | 1      |
        | 2      | 1     | 0        | 0      |

    Examples: three of them, capped on the clusters too small to hold three
        | forced | hosts | replicas | quorum |
        | 3      | 5     | 4        | 3      |
        | 3      | 4     | 3        | 3      |
        | 3      | 3     | 2        | 2      |
        | 3      | 2     | 1        | 1      |

    # Two forced replicas put every commit on both replicas of a three host cluster,
    # so the last host standing has all of them and may be promoted.
    @failover
    Scenario: The last host of a three host cluster is promoted
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'no'
                    quorum_commit: 'yes'
                    forced_quorum_replicas_number: 2
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

    # The same failure while the primary still required a majority: a commit could be on
    # the lost replica only, so the last host must refuse to promote -- even though its
    # own setting says two replicas confirm every commit, as it does while the setting is
    # being rolled out over a cluster.
    @failover
    Scenario: The last host refuses to promote after the primary required a majority
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                global:
                    priority: 0
                    use_replication_slots: 'no'
                    quorum_commit: 'yes'
                    forced_quorum_replicas_number: 2
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
                            forced_quorum_replicas_number: 0
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
