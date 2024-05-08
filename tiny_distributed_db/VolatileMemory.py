import pandas as pd


class VolatileMemory:

    def __init__(self):
        self.tables: dict[str, pd.DataFrame] = {}

    def save_rows(self, table_name: str, rows: pd.DataFrame):
        if table_name in self.tables:
            self.tables = pd.concat([self.tables[table_name], rows])
        else:
            self.tables[table_name] = rows

    def reset(self):
        self.tables = {}

    def extract_rows(self) -> dict[str, pd.DataFrame]:
        return self.tables.copy()
