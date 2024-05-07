import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from tiny_distributed_db.Cluster import Cluster
from utils import random_string, generate_idx_from_uniforme

"""
In this experiment I want to show how table size affect performance on cluster
"""

db_cluster_wl_hist = []
db_network_wl_hist = []
sh_cluster_wl_hist = []
sh_network_wl_hist = []

X = range(1, 100)
for i in X:
    NUMBER_OF_ROW_IN_TABLE_A = i
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

    cluster = Cluster(5)

    cluster.insert("table_a", table_a)
    cluster.insert("table_b", table_b)

    _, workload = cluster.broadcast_join("table_a", "id", "table_b", "fk")
    db_cluster_wl_hist.append(workload.cluster_workload)
    db_network_wl_hist.append(workload.network_workload)

    _, workload = cluster.shuffle_join("table_a", "id", "table_b", "fk")
    sh_cluster_wl_hist.append(workload.cluster_workload)
    sh_network_wl_hist.append(workload.network_workload)
    if i % 10 == 0:
        print(f"Done {i}")


db_cluster_wl_hist_df = pd.DataFrame({
    "Data in table_a": X,
    "Workload": db_cluster_wl_hist,
    "name": ["Broadcast cluster workload"] * len(db_cluster_wl_hist)
})

db_network_wl_hist_df = pd.DataFrame({
    "Data in table_a": X,
    "Workload": db_network_wl_hist,
    "name": ["Broadcast network workload"] * len(db_network_wl_hist)
})

sh_cluster_wl_hist_df = pd.DataFrame({
    "Data in table_a": X,
    "Workload": sh_cluster_wl_hist,
    "name": ["Shuffle cluster workload"] * len(sh_cluster_wl_hist)
})

sh_network_wl_hist_df = pd.DataFrame({
    "Data in table_a": X,
    "Workload": sh_network_wl_hist,
    "name": ["Shuffle network workload"] * len(sh_network_wl_hist)
})

datas = pd.concat([db_cluster_wl_hist_df, db_network_wl_hist_df])
datas = pd.concat([datas, sh_cluster_wl_hist_df])
datas = pd.concat([datas, sh_network_wl_hist_df])

sns.lineplot(data=datas, x='Data in table_a', y='Workload', hue="name")
plt.show()



