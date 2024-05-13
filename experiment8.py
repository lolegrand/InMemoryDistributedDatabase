import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

from tiny_distributed_db.Cluster import Cluster
from utils import random_string, generate_idx_from_normal


def compute_heavy_hitters(table_pk: pd.DataFrame, column_pk: str, table_fk: pd.DataFrame, column_fk: str,
                          threshold: float):
    fanout = table_fk[column_fk].value_counts(normalize=True).reset_index(name="fanout_weight")
    heavy_hitters_id = fanout.loc[fanout["fanout_weight"] > threshold][column_fk]
    table_fk["joinType"] = table_fk[column_fk].apply(lambda x: "broadcast" if x in heavy_hitters_id.values else "shuffle")
    table_pk["joinType"] = table_pk[column_pk].apply(lambda x: "broadcast" if x in heavy_hitters_id.values else "shuffle")

    return table_pk, table_fk


history = {
    "shuffle": {
        "cluster": [],
        "network": []
    },
    "broadcast": {
        "cluster": [],
        "network": []
    },
    "flow": {
        "cluster": [],
        "network": []
    }
}

maxHeavyHitterSize = []

sigmaList = [i / 1000 for i in range(1, 100 + 1)]
for sigma in sigmaList:
    NUMBER_OF_ROW_IN_TABLE_A = 1000
    table_a = pd.DataFrame({
        'id': list(range(NUMBER_OF_ROW_IN_TABLE_A)),
        'string': [random_string() for _ in range(NUMBER_OF_ROW_IN_TABLE_A)]
    })

    NUMBER_OF_ROW_IN_TABLE_B = 1000
    MIN_VALUE = 0
    MAX_VALUE = NUMBER_OF_ROW_IN_TABLE_A - 1
    MU = 0
    table_b = pd.DataFrame({
        'id': list(range(NUMBER_OF_ROW_IN_TABLE_B)),
        'string': [random_string() for _ in range(NUMBER_OF_ROW_IN_TABLE_B)],
        'fk': generate_idx_from_normal(MIN_VALUE,
                                       MAX_VALUE,
                                       NUMBER_OF_ROW_IN_TABLE_B,
                                       MU,
                                       sigma)
    })

    table_a, table_b = compute_heavy_hitters(
        table_a,
        'id',
        table_b,
        'fk',
        0.1
    )

    maxHeavyHitterSize.append(table_b["fk"].value_counts().max())

    cluster = Cluster(10)

    cluster.insert("table_a", table_a)
    cluster.insert("table_b", table_b)

    _, workload = cluster.shuffle_join("table_a", "id", "table_b", "fk")
    history["shuffle"]["cluster"].append(workload.cluster_workload)
    history["shuffle"]["network"].append(workload.network_workload)

    _, workload = cluster.broadcast_join("table_a", "id", "table_b", "fk")
    history["broadcast"]["cluster"].append(workload.cluster_workload)
    history["broadcast"]["network"].append(workload.network_workload)

    _, workload = cluster.flow_join("table_a", "id", "joinType", "table_b", "fk", "joinType")
    history["flow"]["cluster"].append(workload.cluster_workload)
    history["flow"]["network"].append(workload.network_workload)

    print(f"Finished for {sigma} sigma")

sh_cluster_wl_hist_df = pd.DataFrame({
    "Sigma": list(sigmaList),
    "Number of comparison": history["shuffle"]["cluster"],
    "JoinType": ["Shuffle"] * len(history["shuffle"]["cluster"])
})

bc_cluster_wl_hist_df = pd.DataFrame({
    "Sigma": list(sigmaList),
    "Number of comparison": history["broadcast"]["cluster"],
    "JoinType": ["Broadcast"] * len(history["broadcast"]["cluster"])
})

fl_cluster_wl_hist_df = pd.DataFrame({
    "Sigma": list(sigmaList),
    "Number of comparison": history["flow"]["cluster"],
    "JoinType": ["Flow"] * len(history["flow"]["cluster"])
})

sh_network_wl_hist_df = pd.DataFrame({
    "Sigma": list(sigmaList),
    "Number of row transmitted": history["shuffle"]["network"],
    "JoinType": ["Shuffle"] * len(history["shuffle"]["network"])
})

bc_network_wl_hist_df = pd.DataFrame({
    "Sigma": list(sigmaList),
    "Number of row transmitted": history["broadcast"]["network"],
    "JoinType": ["Broadcast"] * len(history["broadcast"]["network"])
})

fl_network_wl_hist_df = pd.DataFrame({
    "Sigma": list(sigmaList),
    "Number of row transmitted": history["flow"]["network"],
    "JoinType": ["Flow"] * len(history["flow"]["network"])
})

max_heavy_hitter_size_df = pd.DataFrame({
    "Max Heavy Hitter Size": maxHeavyHitterSize,
    "Sigma": list(sigmaList)
})

datas_cluster = pd.concat([sh_cluster_wl_hist_df, fl_cluster_wl_hist_df])
datas_network = pd.concat([sh_network_wl_hist_df, fl_network_wl_hist_df])

fig, axs = plt.subplots(ncols=3)
fig.set_size_inches(16, 6)
axs[0].set_title("Cluster workload")
axs[1].set_title("Network workload")
axs[2].set_title("Max heavy hitter size")
sns.lineplot(data=datas_cluster, x="Sigma", y="Number of comparison", hue="JoinType", ax=axs[0])
sns.lineplot(data=datas_network, x="Sigma", y="Number of row transmitted", hue="JoinType", ax=axs[1])
sns.lineplot(data=max_heavy_hitter_size_df, x="Sigma", y="Max Heavy Hitter Size", ax=axs[2])
plt.show()
