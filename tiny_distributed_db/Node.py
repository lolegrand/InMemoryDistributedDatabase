import pandas as pd

from tiny_distributed_db.JointType import JoinType
from tiny_distributed_db.Network import Network, Message
from tiny_distributed_db.VolatileMemory import VolatileMemory


class Node:
    def __init__(self, id: int, number_of_node: int):
        self.number_of_node = number_of_node
        self.id = id
        self.tables: dict[str, pd.DataFrame] = {}
        self.volatileMemory = VolatileMemory()

    def insert(self, table_name: str, rows: pd.DataFrame):
        if table_name in self.tables:
            self.tables = pd.concat([self.tables[table_name], rows])
        else:
            self.tables[table_name] = rows

    def shuffle_table(self, table: str, column: str, network: Network):
        df = self.tables[table]
        grouped = df.groupby(lambda x: df.loc[x, column] % self.number_of_node)
        for i, group in grouped:
            if i != self.id:
                message = Message(self.id, i, table, group)
                network.send_message(message)
            else:
                self.volatileMemory.save_rows(table, group)

    def broadcast_table(self, table: str, network: Network):
        for i in range(self.number_of_node):
            if i != self.id:
                message = Message(self.id, i, table, self.tables[table])
                network.send_message(message)
            else:
                self.volatileMemory.save_rows(table, self.tables[table])

    def load_in_memory(self, table: str):
        self.volatileMemory.save_rows(table, self.tables[table])

    def perform_join(self, table_name_a: str, column_a: str, table_name_b: str, column_b: str, network: Network) -> (pd.DataFrame, int):
        table_a = pd.DataFrame(columns=self.tables[table_name_a].columns)
        table_b = pd.DataFrame(columns=self.tables[table_name_b].columns)

        in_memory_tables = self.volatileMemory.extract_rows()
        if table_name_a in in_memory_tables:
            table_a = in_memory_tables[table_name_a].copy()
        if table_name_b in in_memory_tables:
            table_b = in_memory_tables[table_name_b].copy()
        self.volatileMemory.reset()

        messages = network.receive_message(self.id)
        for message in messages:
            if message.table_name == table_name_a:
                table_a = pd.concat([table_a, message.rows])
            if message.table_name == table_name_b:
                table_b = pd.concat([table_b, message.rows])

        workload = len(table_a.index) * len(table_b.index)
        return pd.merge(table_a, table_b,
                        left_on=column_a,
                        right_on=column_b,
                        how='inner',
                        suffixes=("_" + table_name_a, "_" + table_name_b)), workload
