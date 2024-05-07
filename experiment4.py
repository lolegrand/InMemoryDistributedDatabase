import pandas as pd

from tiny_distributed_db.Cluster import Cluster
from utils import random_string, generate_idx_from_normal

"""
In this experiment I want to how to work with huge dataset.
"""

data = {}
for sigma in [0.25, 0.5, 1.0, 2.0]:

    NUMBER_OF_ROW_IN_TABLE_A = 100
    table_a = pd.DataFrame({
        'id': list(range(NUMBER_OF_ROW_IN_TABLE_A)),
        'string': [random_string() for _ in range(NUMBER_OF_ROW_IN_TABLE_A)],
    })

    NUMBER_OF_ROW_IN_TABLE_B = 100_000
    table_b = pd.DataFrame({
        'id': list(range(NUMBER_OF_ROW_IN_TABLE_B)),
        'string': [random_string() for _ in range(NUMBER_OF_ROW_IN_TABLE_B)],
        'fk': generate_idx_from_normal(0, NUMBER_OF_ROW_IN_TABLE_A, NUMBER_OF_ROW_IN_TABLE_B, 0.0, sigma)
    })

    cluster = Cluster(6)

    cluster.insert("table_a", table_a)
    cluster.insert("table_b", table_b)

    print(f"=== Workload for {sigma} ===")
    df, workload = cluster.shuffle_join("table_a", "id", "table_b", "fk")
    print(workload)



