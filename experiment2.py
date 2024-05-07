import pandas as pd

from tiny_distributed_db.Cluster import Cluster
from utils import random_string, generate_idx_from_uniforme

"""
In this experiment I want to how to work with huge dataset.
"""

NUMBER_OF_ROW_IN_TABLE_A = 250
table_a = pd.DataFrame({
    'id': list(range(NUMBER_OF_ROW_IN_TABLE_A)),
    'string': [random_string() for _ in range(NUMBER_OF_ROW_IN_TABLE_A)],
})

NUMBER_OF_ROW_IN_TABLE_B = 5000
table_b = pd.DataFrame({
    'id': list(range(NUMBER_OF_ROW_IN_TABLE_B)),
    'string': [random_string() for _ in range(NUMBER_OF_ROW_IN_TABLE_B)],
    'fk': generate_idx_from_uniforme(0, NUMBER_OF_ROW_IN_TABLE_A, NUMBER_OF_ROW_IN_TABLE_B)
})


cluster = Cluster(10)

cluster.insert("table_a", table_a)
cluster.insert("table_b", table_b)
print(cluster)

df, workload_bd = cluster.broadcast_join("table_a", "id", "table_b", "fk")
print(len(df.index))
print(workload_bd)

df, workload_sh = cluster.shuffle_join("table_a", "id", "table_b", "fk")
print(len(df.index))
print(workload_sh)

