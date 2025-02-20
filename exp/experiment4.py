import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

from tiny_distributed_db.Cluster import Cluster
from utils import random_string, generate_idx_from_uniforme

"""
Show the impact of the broadcast table on the network workload.
"""

db_cluster_wl_hist = []
db_network_wl_hist = []

X = range(1, 25)
for i in X:
    NUMBER_OF_ROW_IN_TABLE_A = 10
    table_a = pd.DataFrame({
        'id': list(range(NUMBER_OF_ROW_IN_TABLE_A)),
        'string': [random_string() for _ in range(NUMBER_OF_ROW_IN_TABLE_A)],
    })

    NUMBER_OF_ROW_IN_TABLE_B = 250
    table_b = pd.DataFrame({
        'id': list(range(NUMBER_OF_ROW_IN_TABLE_B)),
        'string': [random_string() for _ in range(NUMBER_OF_ROW_IN_TABLE_B)],
        'fk': generate_idx_from_uniforme(0, NUMBER_OF_ROW_IN_TABLE_A, NUMBER_OF_ROW_IN_TABLE_B)
    })

    cluster = Cluster(i)

    cluster.insert("table_a", table_a)
    cluster.insert("table_b", table_b)

    _, workload = cluster.broadcast_join("table_a", "id", "table_b", "fk")

    db_cluster_wl_hist.append(workload.cluster_workload)
    db_network_wl_hist.append(workload.network_workload)

    print(f"Done {i}")

db_network_wl_hist_df = pd.DataFrame({
    "Node on cluster": X,
    "Workload": db_network_wl_hist,
    "name": ["Broadcast network workload"] * len(db_network_wl_hist)
})

db_cluster_wl_hist_df = pd.DataFrame({
    "Node on cluster": X,
    "Workload": db_cluster_wl_hist,
    "name": ["Broadcast cluster workload"] * len(db_cluster_wl_hist)
})

datas = pd.concat([db_network_wl_hist_df, db_cluster_wl_hist_df])

sns.lineplot(data=datas, x='Node on cluster', y='Workload', hue="name")
plt.show()


