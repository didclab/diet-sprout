"""
Constants and Arguments for influx_data and poisson
"""

APP_NAME = "elvisdav@buffalo.edu-didclab-elvis-uc"
USER_NAME = "elvisdav@buffalo.edu"
QUERY_RANGE = "-1m"

NUM_DISCRETE = 255
NUM_WIENER_STEPS = 0
# Should be independent of units
MIN_THROUGHPUT = 0
MAX_THROUGHPUT = 1.25  # Gigabytes

MAX_PARALLELISM = 30
MAX_CONCURRENCY = 30
