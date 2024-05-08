import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

from tiny_distributed_db.Cluster import Cluster
from utils import random_string, generate_idx_from_uniforme

cluster_wl_hist = []
network_wl_hist = []

heavy_hitter_size = range(1000)
for i in heavy_hitter_size:
    # Generate tables
    NUMBER_OF_ROW_IN_TABLE_A = 25
    table_a = pd.DataFrame({
        'id': list(range(NUMBER_OF_ROW_IN_TABLE_A)),
        'string': [random_string() for _ in range(NUMBER_OF_ROW_IN_TABLE_A)],
    })

    NUMBER_OF_ROW_IN_TABLE_B = 1000
    ids = generate_idx_from_uniforme(0, NUMBER_OF_ROW_IN_TABLE_A, NUMBER_OF_ROW_IN_TABLE_B)
    ids[0:i] = [0] * i

    table_b = pd.DataFrame({
        'id': list(range(NUMBER_OF_ROW_IN_TABLE_B)),
        'string': [random_string() for _ in range(NUMBER_OF_ROW_IN_TABLE_B)],
        'fk': ids
    })

    cluster = Cluster(5)

    cluster.insert("table_a", table_a)
    cluster.insert("table_b", table_b)

    _, workload = cluster.shuffle_join("table_a", "id", "table_b", "fk")

    cluster_wl_hist.append(workload.cluster_workload)
    network_wl_hist.append(workload.network_workload)

    if i % 10 == 0:
        print(f"Done {i}")

network_wl_hist_df = pd.DataFrame({
    "Heavy hitter size": list(heavy_hitter_size),
    "Workload": network_wl_hist,
    "name": ["Shuffle network workload"] * len(network_wl_hist)
})

cluster_wl_hist_df = pd.DataFrame({
    "Heavy hitter size": list(heavy_hitter_size),
    "Workload": cluster_wl_hist,
    "name": ["Shuffle cluster workload"] * len(cluster_wl_hist)
})

datas = pd.concat([network_wl_hist_df, cluster_wl_hist_df])

sns.lineplot(data=datas, x='Heavy hitter size', y='Workload', hue="name")
plt.show()
