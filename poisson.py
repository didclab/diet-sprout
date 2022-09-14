import numpy as np
from math import sqrt, floor
from scipy.stats import norm


# https://scipy-cookbook.readthedocs.io/items/BrownianMotion.html

def brownian(x0, n, dt, delta):
    """
    X(t + dt) = X(t) + N(0, delta**2 * dt; t, t+dt)

    :param x0: numpy array Wiener process input
    :param n: Number of steps to take
    :param dt: Time step
    :param delta: "Speed" of Wiener process progression
    :return: numpy array of n steps of Wiener process applied to x0
    """
    x0 = np.asarray(x0)

    # For each x0, generate n samples from normal distribution
    r = norm.rvs(size=x0.shape + (n,), scale=delta * sqrt(dt))

    out = np.empty(r.shape)
    np.cumsum(r, axis=1, out=out)

    return out


class PoissonDistribution:
    def __init__(self, high, num_discrete, num_steps, low=0.):
        self.inputs = np.linspace(low, high, num=num_discrete)
        og_distribution = np.array([1 / num_discrete for _ in range(num_discrete)])
        self.num_discrete = num_discrete

        t = 60  # seconds
        dt = t / num_steps
        delta = 4 / (num_discrete * num_steps)

        self.brownian_motion = brownian(og_distribution, num_steps, dt, delta)
        self.distribution = self.brownian_motion[-1]

    def mean(self):
        return np.sum(self.inputs * self.distribution)

    def update_distribution(self, time, n_bytes):
        n_bytes = floor(n_bytes)
        if n_bytes == 0:
            return
        intermediates = self.distribution * np.power((time * self.inputs), n_bytes) \
            / np.math.factorial(n_bytes)
        intermediates *= np.exp(-time * self.inputs)
        self.distribution = intermediates / np.sum(intermediates)
