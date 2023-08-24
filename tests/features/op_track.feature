Feature: Destructive operation tracking

    Scenario Outline: No lock on primary if unfinished op is present
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                replica:
                    primary_unavailability_timeout: 100500
        """
        And a following cluster with "<lock_type>" without replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
            postgresql3:
                role: replica
        """
        When we set value "rewind" for key "/pgconsul/postgresql/all_hosts/pgconsul_postgresql1_1.pgconsul_pgconsul_net/op" in <lock_type> "<lock_host>"
        Then <lock_type> "<lock_host>" has holder "None" for lock "/pgconsul/postgresql/leader"
        And pgbouncer is not running in container "postgresql1"

    Examples: <lock_type>
        |   lock_type   |   lock_host   |
        |   zookeeper   |   zookeeper1  |

    Scenario Outline: Unfinished op is properly cleaned up on replica
        Given a "pgconsul" container common config
        """
            pgconsul.conf:
                replica:
                    primary_unavailability_timeout: 100500
        """
        And a following cluster with "<lock_type>" without replication slots
        """
            postgresql1:
                role: primary
            postgresql2:
                role: replica
            postgresql3:
                role: replica
        """
        When we set value "rewind" for key "/pgconsul/postgresql/all_hosts/pgconsul_postgresql2_1.pgconsul_pgconsul_net/op" in <lock_type> "<lock_host>"
        Then <lock_type> "<lock_host>" has value "None" for key "/pgconsul/postgresql/all_hosts/pgconsul_postgresql2_1.pgconsul_pgconsul_net/op"

    Examples: <lock_type>
        |   lock_type   |   lock_host   |
        |   zookeeper   |   zookeeper1  |
