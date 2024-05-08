import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

from tiny_distributed_db.Cluster import Cluster
from utils import random_string, generate_idx_from_uniforme

sh_cluster_wl_hist = []
bc_cluster_wl_hist = []

sh_network_wl_hist = []
bc_network_wl_hist = []

NUMBER_OF_ROW_IN_TABLE_B = 100
heavy_hitter_size = range(NUMBER_OF_ROW_IN_TABLE_B)
for i in heavy_hitter_size:
    NUMBER_OF_ROW_IN_TABLE_A = 50
    table_a = pd.DataFrame({
        'id': list(range(NUMBER_OF_ROW_IN_TABLE_A)),
        'string': [random_string() for _ in range(NUMBER_OF_ROW_IN_TABLE_A)],
    })

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
    sh_cluster_wl_hist.append(workload.cluster_workload)
    sh_network_wl_hist.append(workload.network_workload)

    _, workload = cluster.broadcast_join("table_a", "id", "table_b", "fk")
    bc_cluster_wl_hist.append(workload.network_workload)
    bc_network_wl_hist.append(workload.network_workload)

    if i % 10 == 0:
        print(f"Done {i}")

sh_cluster_wl_hist_df = pd.DataFrame({
    "Heavy hitter size": list(heavy_hitter_size),
    "Nbr of comparison": sh_cluster_wl_hist,
    "JoinType": ["Shuffle"] * len(sh_cluster_wl_hist)
})

bc_cluster_wl_hist_df = pd.DataFrame({
    "Heavy hitter size": list(heavy_hitter_size),
    "Nbr of comparison": bc_cluster_wl_hist,
    "JoinType": ["Broadcast"] * len(bc_cluster_wl_hist)
})

sh_network_wl_hist_df = pd.DataFrame({
    "Heavy hitter size": list(heavy_hitter_size),
    "Number of row transmitted": sh_network_wl_hist,
    "JoinType": ["Shuffle"] * len(sh_network_wl_hist)
})

bc_network_wl_hist_df = pd.DataFrame({
    "Heavy hitter size": list(heavy_hitter_size),
    "Number of row transmitted": bc_network_wl_hist,
    "JoinType": ["Broadcast"] * len(bc_network_wl_hist)
})

datas_cluster = pd.concat([sh_cluster_wl_hist_df, bc_cluster_wl_hist_df])
datas_network = pd.concat([sh_network_wl_hist_df, bc_network_wl_hist_df])

fig, axs = plt.subplots(ncols=2)
fig.set_size_inches(16, 6)
axs[0].set_title("Cluster workload")
axs[1].set_title("Network workload")
sns.lineplot(data=datas_cluster, x="Heavy hitter size", y="Nbr of comparison", hue="JoinType", ax=axs[0])
sns.lineplot(data=datas_network, x="Heavy hitter size", y="Number of row transmitted", hue="JoinType", ax=axs[1])
plt.show()
