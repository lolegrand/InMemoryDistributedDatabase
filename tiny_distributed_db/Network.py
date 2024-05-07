import pandas as pd


class Message:
    def __init__(self, source_node_id: int, target_node_id: int, table_name: str, rows: pd.DataFrame):
        self.source_node_id = source_node_id
        self.target_node_id = target_node_id
        self.table_name = table_name
        self.rows = rows


class Network:
    def __init__(self):
        self.message = []
        self.nbr_of_row_transmitted = 0

    def send_message(self, message: Message):
        self.message.append(message)
        self.nbr_of_row_transmitted += len(message.rows.index)

    def receive_message(self, target_id: int) -> list[Message]:
        output = [m for m in self.message if m.target_node_id == target_id]
        for m in output:
            self.message.remove(m)
        return output

    def get_my_message(self, source_id: int) -> list[Message]:
        output = [m for m in self.message if m.source_node_id == source_id]
        for m in output:
            self.message.remove(m)
        return output
