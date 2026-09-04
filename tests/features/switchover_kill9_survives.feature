# Disabled until the manager-election race can be controlled deterministically.
# These cases belong in fault-injection tests.
#
# Feature: Switchover survives pgconsul kill -9 in mid-phases
#
#     @switchover
#     Scenario: Kill -9 pgconsul on primary in turning-sides phase
#         Given a "pgconsul" container common config
#         """
#             pgconsul.conf:
#                 global:
#                     priority: 0
#                     use_replication_slots: 'yes'
#                     quorum_commit: 'yes'
#                 primary:
#                     change_replication_type: 'yes'
#                     primary_switch_checks: 3
#                 replica:
#                     primary_switch_checks: 3
#                     min_failover_timeout: 120
#                     primary_unavailability_timeout: 10
#                 commands:
#                     generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
#         """
#         Given a following cluster with "zookeeper" with replication slots
#         """
#             postgresql1:
#                 role: primary
#                 config:
#                     pgconsul.conf:
#                         global:
#                             priority: 3
#             postgresql2:
#                 role: replica
#                 config:
#                     pgconsul.conf:
#                         global:
#                             priority: 1
#             postgresql3:
#                 role: replica
#                 config:
#                     pgconsul.conf:
#                         global:
#                             priority: 2
#         """
#         Then container "postgresql3" is in quorum group
#         When we do targeted switchover from container "postgresql1" to container "postgresql2"
#         Then zookeeper "zookeeper1" has switchover phase "turning_sides"
#         When we kill "pgconsul" in container "postgresql1" with signal "SIGKILL"
#         And we wait "15.0" seconds
#         And we start "pgconsul" in container "postgresql1"
#         Then container "postgresql2" became a primary
#         And container "postgresql3" is a replica of container "postgresql2"
#         And container "postgresql1" is a replica of container "postgresql2"
#         And container "postgresql1" is in quorum group
#         And postgresql in container "postgresql3" was not rewinded
#         And postgresql in container "postgresql1" was rewinded
#         And timing log in container "postgresql2" contains "switchover,downtime"
#
#     @switchover
#     Scenario: Kill -9 pgconsul on primary in preparing-durability phase
#         Given a "pgconsul" container common config
#         """
#             pgconsul.conf:
#                 global:
#                     priority: 0
#                     use_replication_slots: 'yes'
#                     quorum_commit: 'yes'
#                 primary:
#                     change_replication_type: 'yes'
#                     primary_switch_checks: 3
#                 replica:
#                     primary_switch_checks: 3
#                     min_failover_timeout: 120
#                     primary_unavailability_timeout: 10
#                 commands:
#                     generate_recovery_conf: /usr/local/bin/gen_rec_conf_with_slot.sh %m %p
#         """
#         Given a following cluster with "zookeeper" with replication slots
#         """
#             postgresql1:
#                 role: primary
#                 config:
#                     pgconsul.conf:
#                         global:
#                             priority: 3
#             postgresql2:
#                 role: replica
#                 config:
#                     pgconsul.conf:
#                         global:
#                             priority: 1
#             postgresql3:
#                 role: replica
#                 config:
#                     pgconsul.conf:
#                         global:
#                             priority: 2
#         """
#         Then container "postgresql3" is in quorum group
#         When we do targeted switchover from container "postgresql1" to container "postgresql2"
#         Then zookeeper "zookeeper1" has switchover phase "preparing_durability"
#         When we kill "pgconsul" in container "postgresql1" with signal "SIGKILL"
#         And we wait "15.0" seconds
#         And we start "pgconsul" in container "postgresql1"
#         Then container "postgresql2" became a primary
#         And container "postgresql3" is a replica of container "postgresql2"
#         And container "postgresql1" is a replica of container "postgresql2"
#         And container "postgresql1" is in quorum group
#         And postgresql in container "postgresql3" was not rewinded
#         And postgresql in container "postgresql1" was rewinded
#         And timing log in container "postgresql2" contains "switchover,downtime"
