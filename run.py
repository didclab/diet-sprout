import constants as cons
from poisson import PoissonDistribution
import influx_data


class ParameterDistributionMap:
    def __init__(self):
        self.PD_map = {}
        for p in range(1, cons.MAX_PARALLELISM+1):
            for c in range(1, cons.MAX_CONCURRENCY+1):
                self.PD_map[(p, c)] = PoissonDistribution(
                    cons.MAX_THROUGHPUT,
                    cons.NUM_DISCRETE,
                    cons.NUM_WIENER_STEPS
                )

    def update_parameter_dist(self, p, c, n_units, time=30):
        if p == 0 or c == 0:
            return
        distribution = self.PD_map.get((p, c))
        if distribution is None:
            self.PD_map[(p, c)] = PoissonDistribution(
                    cons.MAX_THROUGHPUT,
                    cons.NUM_DISCRETE,
                    cons.NUM_WIENER_STEPS
            )
        else:
            distribution.update_distribution(time, n_units)
