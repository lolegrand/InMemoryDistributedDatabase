from tiny_distributed_db import JointType


class Workload:
    def __init__(self, network_workload: int, cluster_workload: int, nodes_workload: dict[int, int], join_type: JointType):
        self.join_type = join_type
        self.nodes_workload = nodes_workload
        self.cluster_workload = cluster_workload
        self.network_workload = network_workload

    def __str__(self):
        out = ""
        out += f"====== {self.join_type} ======\n"
        out += f"Network row transfer : {self.network_workload}\n"
        out += f"Cluster node workload : {self.cluster_workload}\n"
        for i, w in self.nodes_workload.items():
            out += f" - Node {i} : {w}\n"
        out += "\n"
        return out
