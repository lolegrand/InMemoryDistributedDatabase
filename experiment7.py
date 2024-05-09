import numpy as np
import pandas
import seaborn as sns
from matplotlib import pyplot as plt
from matplotlib.animation import FuncAnimation

from utils import generate_idx_from_normal

min_value = 0
max_value = 200
num_frame = 25
mu_value = 0
sigma_value = np.linspace(0.001, 1.0, num_frame)
size_of_sample = 10000

fig, ax = plt.subplots(ncols=2)
fig.set_size_inches(16, 6)


def update(frame):
    fig.suptitle(f"Data distribution for sigma={sigma_value[frame]}", fontsize=16)

    ax[0].clear()
    ax[1].clear()

    ax[0].set_xlim([min_value, max_value])

    ids = pandas.DataFrame({
        "ids": generate_idx_from_normal(min_value, max_value, size_of_sample, mu_value, sigma_value[frame])
    })

    fanout_freq = ids.value_counts() \
        .reset_index(name="fanout")["fanout"] \
        .value_counts(normalize=True) \
        .reset_index(name="frequency")

    sns.histplot(ids, x="ids", ax=ax[0])
    sns.lineplot(fanout_freq, x="fanout", y="frequency", ax=ax[1])


ani = FuncAnimation(fig, update, frames=num_frame, repeat=True)
plt.show()
