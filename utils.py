import random
import string
import seaborn as sns
import numpy as np
from matplotlib import pyplot as plt


def random_string(length_min=8, length_max=12):
    longueur = random.randint(length_min, length_max)
    char = string.ascii_letters + string.digits
    return ''.join(random.choice(char) for _ in range(longueur))


def generate_idx_from_normal(min_val: int, max_val: int, size: int, mu: float, sigma: float):
    mean = (max_val + min_val) / 2
    std_dev = (max_val - min_val) / 6

    list_value = []
    while len(list_value) != size:
        nbr = round(std_dev * random.gauss(mu, sigma) + mean)
        if min_val <= nbr <= max_val:
            list_value.append(nbr)

    return np.array(list_value)


def generate_idx_from_uniforme(min_val, max_val, size):
    return [random.randint(min_val, max_val -1) for _ in range(size)]


def display_data_fanout_frequency(data):
    fanout = data.value_counts().reset_index(name='fanout')['fanout']
    frequency = fanout.value_counts(normalize=True).reset_index(name='frequency')
    sns.lineplot(frequency, x="fanout", y="frequency")
    plt.show()


def display_data_distribution(datas):
    sns.histplot(datas)
    plt.show()
