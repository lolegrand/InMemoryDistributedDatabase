from functools import reduce

import pandas as pd

from tiny_distributed_db.JointType import JoinType
from tiny_distributed_db.Network import Network
from tiny_distributed_db.Node import Node
from tiny_distributed_db.Workload import Workload


class Cluster:
    def __init__(self, number_of_nodes: int):
        self.number_of_nodes = number_of_nodes
        self.nodes = [Node(i, self.number_of_nodes) for i in range(number_of_nodes)]

    def insert(self, table_name: str, rows: pd.DataFrame):
        df = rows.sample(frac=1)
        chunk_size, remainder = divmod(len(df), self.number_of_nodes)
        chunk_df = []

        start_idx = 0
        for i in range(self.number_of_nodes):
            part_size = chunk_size + 1 if i < remainder else chunk_size
            chunk_df.append(df.iloc[start_idx:start_idx + part_size])
            start_idx += part_size

        for idx, node in enumerate(self.nodes):
            node.insert(table_name, chunk_df[idx])

    def broadcast_join(self, table_a: str, column_a: str, table_b: str, column_b: str) -> (pd.DataFrame, Workload):
        network = Network()
        for node in self.nodes:
            node.broadcast_table(table_a, network)

        return self._perform_join(table_a, column_a, table_b, column_b, network, JoinType.BROADCAST)

    def shuffle_join(self, table_a: str, column_a: str, table_b: str, column_b: str) -> (pd.DataFrame, Workload):
        network = Network()
        for node in self.nodes:
            node.shuffle_table(table_a, column_a, network)
            node.shuffle_table(table_b, column_b, network)

        return self._perform_join(table_a, column_a, table_b, column_b, network, JoinType.SHUFFLE)

    def _perform_join(self, table_a: str, column_a: str, table_b: str, column_b: str, network: Network, join_type: JoinType) -> (pd.DataFrame, Workload):
        dfs = []
        workload_hist = {}
        for node in self.nodes:
            df, workload = node.perform_join(table_a, column_a, table_b, column_b, network, join_type)
            workload_hist[node.id] = workload
            dfs.append(df)

        workload = Workload(network.nbr_of_row_transmitted, max(workload_hist.values()), workload_hist, join_type)

        return reduce(lambda x, y: pd.concat([x, y]), dfs), workload

    def __str__(self):
        out = ""
        for node in self.nodes:
            out += f"==== Node {node.id} ====\n"
            for table_name, rows in node.tables.items():
                out += f" - {table_name}, {len(rows)}\n"
        out += "\n"
        return out
