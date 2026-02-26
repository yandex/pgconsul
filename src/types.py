from dataclasses import dataclass

PluginsConfig = dict[str, str]
ReplicaInfo = dict[str, int | str]
ReplicaInfos = list[ReplicaInfo]


@dataclass
class ClusterInfo:
    version: int
    cluster_name: str
    port: str
    state: str
    owner: str
    pgdata: str
    log_file: str
