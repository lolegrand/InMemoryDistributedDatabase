import random

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np

from utils import generate_idx_from_normal

A = 0
B = 500
lambda_ = 1.0
size = 1000
data = generate_idx_from_normal(A, B, 1000, 0.0, 1.0)

sns.histplot(data)
plt.show()

