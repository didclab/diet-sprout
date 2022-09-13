import constants as cons
import pandas as pd
import numpy as np
from math import sqrt
from scipy.stats import norm

# https://scipy-cookbook.readthedocs.io/items/BrownianMotion.html

class PoissonDistribution:
    def __init__(self, high, num_discrete, low=0.):
        self.inputs = np.linspace(low, high, num=num_discrete)
        self.distribution = np.array([1 / num_discrete for _ in range(num_discrete)])

    def mean(self):
        pass

    def update_distribution(self, time, n_bytes):
        pass
