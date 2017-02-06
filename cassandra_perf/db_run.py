

import multiprocessing

import cassandra
from cassandra.cluster import Cluster
from cassandra.io.asyncorereactor import AsyncoreConnection
from benchmarks.BenchmarkObject import BenchmarkObject

import logging
import time

from greplin import scales
import argparse

log = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

logging.getLogger('cassandra').setLevel(logging.WARN)

_log_levels = {
    'CRITICAL': logging.CRITICAL,
    'ERROR': logging.ERROR,
    'WARN': logging.WARNING,
    'WARNING': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
    'NOTSET': logging.NOTSET,
}


KEYSPACE = 'dbao_ad_analytics_realtime'
TABLE = 'campaign_analytics_hourly_offset'

COLUMNS_MAP = {
    'campaign_id': 'int',
    'hour': 'int',
    'clicks_count': 'int',
    'kafka_partition': 'int',
    'kafka_offset': 'int',
}

PRIMARY_KEY = "(campaign_id, hour, kafka_partition)"


def setup(args):
    log.info("Using 'cassandra' package from %s", cassandra.__path__)

    cluster = Cluster(args.hosts, schema_metadata_enabled=False,
                      token_metadata_enabled=False)
    try:
        session = cluster.connect()

        rows = session.execute(
            "SELECT keyspace_name FROM system.schema_keyspaces")

        if KEYSPACE in [row[0] for row in rows]:
            log.info("Dropping existing keyspace...")
            session.execute("DROP KEYSPACE " + KEYSPACE)

        log.info("Creating keyspace {} ...".format(KEYSPACE))
        try:
            session.execute("""
                CREATE KEYSPACE %s
                WITH replication = { 'class': 'NetworkTopologyStrategy',
                'norcal-devc': '3' }
                """ % KEYSPACE)
        except cassandra.AlreadyExists:
            log.debug("Keyspace already exists")

        session.set_keyspace(KEYSPACE)

        log.info("Creating table {} ...".format(TABLE))
        create_table_query = """
            CREATE TABLE {0} (
        """
        for col_name, col_type in COLUMNS_MAP.iteritems():
            create_table_query += \
                "{col_name} {col_type},\n".format(col_name=col_name,
                                                  col_type=col_type)
        create_table_query += "PRIMARY KEY {pk})".format(pk=PRIMARY_KEY)

        # log.debug("create table query {}".format(create_table_query))
        try:
            session.execute(create_table_query.format(TABLE))
        except cassandra.AlreadyExists:
            log.debug("Table already exists.")

        log.info("Inserting {} keys ... ".format(args.num_keys))
        insert_data(session, args.num_keys, args.num_processes, 2)

    finally:
        cluster.shutdown()


def insert_data(session, num_keys, num_processes, num_partition):

    cols = "(campaign_id, hour, kafka_partition, clicks_count, " \
           "kafka_offset)"

    prepared_query = "INSERT INTO {table} {cols} VALUES (?,?,?,?,?)".format(
                    table=TABLE,
                    cols=cols)
    prepared = session.prepare(prepared_query)

    for campaign_id in range(num_keys):
        for hour in range(num_processes):
            for kafka_partition in range(num_partition):
                session.execute(prepared.bind((campaign_id, hour,
                                              kafka_partition, 0, 0)))


def teardown(args):
    cluster = Cluster(args.hosts, schema_metadata_enabled=False,
                      token_metadata_enabled=False)
    session = cluster.connect()
    if not args.keep_data:
        session.execute("DROP KEYSPACE " + KEYSPACE)
    cluster.shutdown()


def parse_options():
    parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--hosts', default='127.0.0.1',
                        help='cassandra hosts to connect to (comma-separated '
                             'list) [default: %default]')
    parser.add_argument('-p', '--num-processes', type=int, default=1,
                        help='number of processes [default: %default]')
    parser.add_argument('-n', '--num-ops', type=int, default=10000,
                        help='number of operations [default: %default]')
    parser.add_argument('-l', '--log-level', default='info',
                        help='logging level: debug, info, warning, or error')
    parser.add_argument('-P', '--profile', action='store_true', dest='profile',
                        help='Profile the run')
    parser.add_argument('--protocol-version', type=int,
                        dest='protocol_version', default=4,
                        help='Native protocol version to use')
    parser.add_argument('-k', '--num-keys', type=int, default=200,
                        help='number of keys [default: %default]')
    parser.add_argument('--keep-data', action='store_true', default=False,
                        help='Keep the data after the benchmark')
    parser.add_argument('--no-setup', action='store_true', default=False,
                        help='Don\'t recreate table for the benchmark')

    args = parser.parse_args()

    args.hosts = args.hosts.split(',')

    return args


def apply_load(benchmark_object):
    """

    :param benchmark_objects:
    :return:
    """
    # TODO: remove options
    hosts = benchmark_object.hosts
    kwargs = benchmark_object.kwargs
    keyspace = benchmark_object.keyspace
    process_num = benchmark_object.process_num
    num_queries = benchmark_object.num_queries

    log.info("start {}".format(multiprocessing.current_process().name))

    cluster = Cluster(hosts, **kwargs)
    session = cluster.connect(keyspace)

    futures = []
    benchmark_object.start_profile()

    for i in range(num_queries):
        future = benchmark_object.run_query(process_num,
                                            session)
        futures.append(future)

    for future in futures:
        try:
            future.result()
        except Exception:
            pass

    benchmark_object.finish_profile()

    cluster.shutdown()
    stats = scales.getStats()['cassandra']

    log.info("Connection errors: %d", stats['connection_errors'])
    log.info("Write timeouts: %d", stats['write_timeouts'])
    log.info("Read timeouts: %d", stats['read_timeouts'])
    log.info("Unavailables: %d", stats['unavailables'])
    log.info("Other errors: %d", stats['other_errors'])
    log.info("Retries: %d", stats['retries'])

    request_timer = stats['request_timer']
    log.info("Request latencies:")
    log.info("  min: %0.4fs", request_timer['min'])
    log.info("  max: %0.4fs", request_timer['max'])
    log.info("  mean: %0.4fs", request_timer['mean'])
    log.info("  stddev: %0.4fs", request_timer['stddev'])
    log.info("  median: %0.4fs", request_timer['median'])
    log.info("  75th: %0.4fs", request_timer['75percentile'])
    log.info("  95th: %0.4fs", request_timer['95percentile'])
    log.info("  98th: %0.4fs", request_timer['98percentile'])
    log.info("  99th: %0.4fs", request_timer['99percentile'])
    log.info("  99.9th: %0.4fs", request_timer['999percentile'])


def benchmark():
    args = parse_options()

    level = args.log_level.upper()
    try:
        log.setLevel(_log_levels[level])
    except KeyError:
        log.warn("Unknown log level specified: %s; specify one of %s",
                 args.log_level, _log_levels.keys())

    conn_class = AsyncoreConnection
    kwargs = {'metrics_enabled': True,
              'connection_class': conn_class}
    if args.protocol_version:
        kwargs['protocol_version'] = args.protocol_version

    if not args.no_setup:
        log.info("Setting up keyspace {} and table {}".format(KEYSPACE, TABLE))
        setup(args)

    log.debug("Sleeping for two seconds...")
    time.sleep(2.0)

    num_per_process = args.num_ops // args.num_processes

    log.info(
        "Beginning {0}...".format('inserts')
    )
    log.info("==== {} ====".format(conn_class.__name__,))

    # remove cluster setup/close time? like take the max of start/end time?
    start = time.time()

    jobs = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(jobs)

    benchmark_object_list = [BenchmarkObject(i,  # process number
                                             kwargs,
                                             args.hosts,
                                             num_per_process,
                                             args.num_keys,
                                             args.profile,
                                             output_metrics_q)
                             for i in range(args.num_processes)
                             ]

    results = pool.map_async(apply_load, benchmark_object_list)
    results.get()
    pool.close()
    pool.join()

    end = time.time()
    teardown(args)

    total = end - start
    log.info("Total time: %0.2fs" % total)
    log.info("Average throughput: %0.2f/sec" % (args.num_ops / total))


class BenchmarkObject:
    TOTAL_KAFKA_PARTITION = 5

    def __init__(self, process_num, kwargs, hosts,
                 keyspace, query, values,
                 num_queries, num_keys, profile):
        self.process_num = process_num
        self.hosts = hosts
        self.kwargs = kwargs
        self.keyspace = keyspace
        self.query = query  # TODO: use prepare_statement?
        # abstraction: no need
        self.values = values
        self.num_queries = num_queries
        self.num_keys = num_keys  # to scale
        self.profiler = Profile() if profile else None

    def start_profile(self):
        if self.profiler:
            self.profiler.enable()

    def finish_profile(self):
        if self.profiler:
            self.profiler.disable()
            self.profiler.dump_stats('profile-%d' % self.process_num)

    # TODO: this is a overridable function
    def run_query(self, process_num, session):
        """ run the read-modify-write query to update counter
        :param key:
        :return: a future to wait for result
        """

        # randomly generate a key between range
        # for simiplicity, just assume 200 campaign_id,
        # (cid, hour) is the key, so just let hour be tied to process number
        campaign_id = random.randint(0, self.num_keys + 1)
        hour = process_num

        try:
            # synchronously read current count
            # kafka_partition = random.randint(0, self.TOTAL_KAFKA_PARTITION)
            # tmp
            kafka_partition = 0
            cond = "campaign_id={cid} and hour={hour} and kafka_partition={" \
                   "kafka_partition}".format(cid=campaign_id,
                                             hour=hour,
                                             kafka_partition=kafka_partition)

            read_query = "SELECT {col1},{col2} " \
                         "FROM {table} WHERE {cond}".format(
                                col1='kafka_offset',
                                col2='clicks_count',
                                table=TABLE,
                                cond=cond)

            result = session.execute(read_query)

            # run lwt query to update count
            old_offset = result[0].kafka_offset
            old_count = result[0].clicks_count
            lwt_query = "UPDATE {table} SET kafka_offset={new_offset}, " \
                        "clicks_count={new_count} " \
                        "WHERE {cond} " \
                        "if kafka_offset={old_offset}".format(
                            table=TABLE,
                            new_count=old_count + 5,
                            new_offset=old_offset + 1,
                            cond=cond,
                            old_offset=old_offset)

            return session.execute_async(lwt_query)
        except Exception:
            print("Exception in worker:")
            traceback.print_exc()
            raise

if __name__ == "__main__":
    benchmark()