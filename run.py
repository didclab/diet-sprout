import constants as cons
from poisson import PoissonDistribution
from influx_data import InfluxDataMini


class ParameterDistributionMap:
    def __init__(self):
        self.PD_map = {}
        for p in range(1, cons.MAX_PARALLELISM + 1):
            for c in range(1, cons.MAX_CONCURRENCY + 1):
                self.PD_map[(p, c)] = PoissonDistribution(
                    cons.MAX_THROUGHPUT,
                    cons.NUM_DISCRETE,
                    cons.NUM_WIENER_STEPS
                )
        self.recommendation = (2, 2)
        self.total_updates = 0

    def prune(self):
        if self.total_updates < 480:
            return

        popped_keys = []

        for p, c in self.PD_map:
            dist = self.PD_map[(p, c)]
            dist_updates = dist.num_updates

            if dist_updates / self.total_updates < 0.01:
                popped_keys.append((p, c))
            dist.num_updates = 0

        for key in popped_keys:
            self.PD_map.pop(key, None)

        self.total_updates = 0

    def get_best_parameter(self):
        return self.recommendation

    def calculate_best_parameter(self):
        best_parameter = (0, 0)
        mean = 0

        for p, c in self.PD_map:
            cur_mean = self.PD_map[(p, c)].mean()
            if mean < cur_mean:
                mean = cur_mean
                best_parameter = (p, c)

        return best_parameter

    def update_parameter_dist(self, p, c, n_units, time=30):
        if p == 0 or c == 0:
            return
        distribution = self.PD_map.get((p, c))
        if distribution is None:
            distribution = PoissonDistribution(
                cons.MAX_THROUGHPUT,
                cons.NUM_DISCRETE,
                cons.NUM_WIENER_STEPS
            )
            self.PD_map[(p, c)] = distribution

        distribution.update_distribution(time, n_units)

        self.recommendation = self.calculate_best_parameter()
        self.total_updates += 1


def simple_apply(x, dist_map):
    p = x['parallelism']
    c = x['concurrency']
    n_units = (x['throughput'] * 1e-9 / 8) * 30
    dist_map.update_parameter_dist(p, c, n_units)
    return x


def main():
    data_handler = InfluxDataMini(file_name='pivot.csv')
    data = data_handler.prune_df(data_handler.read_file())

    parameter_dist_map = ParameterDistributionMap()
    print(parameter_dist_map.get_best_parameter())

    live_data = data[(data['parallelism'] > 0.) & (data['throughput'] > 0.)].filter(
        items=['parallelism', 'concurrency', 'throughput']
    )
    live_data.apply(lambda x: simple_apply(x, parameter_dist_map), axis=1)
    print(parameter_dist_map.get_best_parameter())


if __name__ == "__main__":
    main()
