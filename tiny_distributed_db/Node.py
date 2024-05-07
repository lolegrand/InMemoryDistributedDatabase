import pandas as pd

from tiny_distributed_db.JointType import JoinType
from tiny_distributed_db.Network import Network, Message


class Node:
    def __init__(self, id: int, number_of_node: int):
        self.number_of_node = number_of_node
        self.id = id
        self.tables: dict[str, pd.DataFrame] = {}

    def insert(self, table_name: str, rows: pd.DataFrame):
        if table_name in self.tables:
            self.tables = pd.concat([self.tables[table_name], rows])
        else:
            self.tables[table_name] = rows

    def broadcast_table(self, table: str, network: Network):
        for i in range(self.number_of_node):
            if i != self.id:
                message = Message(self.id, i, table, self.tables[table])
                network.send_message(message)

    def shuffle_table(self, table: str, column: str, network: Network):
        df = self.tables[table]
        grouped = df.groupby(lambda x: df.loc[x, column] % self.number_of_node)
        for i, group in grouped:
            if i != self.id:
                message = Message(self.id, i, table, group)
                network.send_message(message)

    def perform_join(self, table_name_a: str, column_a: str, table_name_b: str, column_b: str, network: Network, join_type: JoinType) -> (pd.DataFrame, int):
        table_a = self.tables[table_name_a].copy()
        table_b = self.tables[table_name_b].copy()

        messages = network.receive_message(self.id)
        for message in messages:
            if message.table_name == table_name_a:
                table_a = pd.concat([table_a, message.rows])
            elif message.table_name == table_name_b:
                table_b = pd.concat([table_b, message.rows])

        if join_type == JoinType.SHUFFLE:
            table_a = table_a[table_a[column_a] % self.number_of_node == self.id]
            table_b = table_b[table_b[column_b] % self.number_of_node == self.id]

        workload = len(table_a.index) * len(table_b.index)
        return pd.merge(table_a, table_b,
                        left_on=column_a,
                        right_on=column_b,
                        how='inner',
                        suffixes=("_" + table_name_a, "_" + table_name_b)), workload
