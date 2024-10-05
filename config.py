# config.py

PATHS = {
    'CSV_PATH': '/path/to/csv',
    'DUMMY_PATH': '/path/to/dummy',
    'LOG_PATH': '/path/to/log'
}

CASSANDRA_HOST_FOR_LOADING = 'localhost'
CQLSH_TIMEOUT_IN_SECONDS = 30
LOAD_PROCESS_POOL_SIZE = 4
LOAD_PERIOD_IN_DAYS = 7
LOAD_SLEEP_IN_SECONDS = 600
MAIN_LOOP_SLEEP_IN_SECONDS = 3600

CSV_HEADER = ['column1', 'column2', 'column3']  # Adjust as per your CSV structure

LOG_FORMAT = "%(asctime)s|%(process)s|%(processName)s|%(thread)s|%(threadName)s|%(module)s|%(name)s|%(levelname)s|%(message)s"
