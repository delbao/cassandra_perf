

import multiprocessing

import cassandra
from cassandra.cluster import Cluster
from cassandra.io.asyncorereactor import AsyncoreConnection
from benchmarks.BenchmarkObject import BenchmarkObject

import logging
import time
import sys


from greplin import scales
import argparse

from collections import defaultdict


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

    jobs = multiprocessing.cpu_count()
    output_metrics_q = multiprocessing.Manager().Queue()
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

    process_metrics(output_metrics_q)

    teardown(args)


def process_metrics(output_metrics_q):
    min_start, max_end = sys.maxint, 0
    count_stats = defaultdict(int)
    latency_stats = defaultdict(float)

    while not output_metrics_q.empty():
        # count_stats:
        # {'count': 30, 'retries': 0, 'read_timeouts': 0, 'write_timeouts':
        # 0, 'connection_errors': 0, 'other_errors': 0})

        # latency_stats:
        # {'99percentile': 0.009811162948608398, '75percentile':
        # 0.009811162948608398, '95percentile': 0.009811162948608398, 'max':
        # 0.009811162948608398, '98percentile': 0.009811162948608398, 'min':
        # 0.007244110107421875, 'median': 0.007951140403747559,
        # '999percentile': 0.009811162948608398, 'stddev':
        # 0.0030483972590959435, 'mean': 0.007951140403747559})

        stats, start, end = output_metrics_q.get()
        min_start = min(start, min_start)
        max_end = max(end, max_end)
        for key, value in stats.iteritems():
            if key == 'request_timer':
                for lkey, fval in value.iteritems():
                    if lkey == 'count':
                        count_stats[lkey] += fval
                    else:
                        latency_stats[lkey] = max(fval,
                                                  latency_stats[lkey])
            else:
                count_stats[key] += value


    log.info("Connection errors: %d", count_stats['connection_errors'])
    log.info("Write timeouts: %d", count_stats['write_timeouts'])
    log.info("Read timeouts: %d", count_stats['read_timeouts'])
    log.info("Unavailables: %d", count_stats['unavailables'])
    log.info("Other errors: %d", count_stats['other_errors'])
    log.info("Retries: %d", count_stats['retries'])

    log.info("Request latencies:")
    log.info("  min: %0.4fs", latency_stats['min'])
    log.info("  max: %0.4fs", latency_stats['max'])
    log.info("  mean: %0.4fs", latency_stats['mean'])
    log.info("  stddev: %0.4fs", latency_stats['stddev'])
    log.info("  median: %0.4fs", latency_stats['median'])
    log.info("  75th: %0.4fs", latency_stats['75percentile'])
    log.info("  95th: %0.4fs", latency_stats['95percentile'])
    log.info("  98th: %0.4fs", latency_stats['98percentile'])
    log.info("  99th: %0.4fs", latency_stats['99percentile'])
    log.info("  99.9th: %0.4fs", latency_stats['999percentile'])

    total = max_end - min_start
    log.info("Total time: %0.2fs" % total)
    log.info("Average throughput: %0.2f/sec" % (count_stats['count'] / total))


def apply_load(benchmark_object):
    """

    :param benchmark_objects:
    :return:
    """

    hosts = benchmark_object.hosts
    kwargs = benchmark_object.kwargs
    num_queries = benchmark_object.num_queries
    output_metrics_q = benchmark_object.output_metrics_q

    log.debug("start {}".format(multiprocessing.current_process().name))

    cluster = Cluster(hosts, **kwargs)
    session = cluster.connect(KEYSPACE)

    futures = []
    benchmark_object.start_profile()

    start = time.time()

    for i in range(num_queries):
        future = benchmark_object.run_query(session)
        futures.append(future)

    for future in futures:
        try:
            future.result()
        except Exception:
            pass

    end = time.time()
    benchmark_object.finish_profile()

    # cant pass stats back directly as it has lambda (cannot pickle)
    cass_stats = {}
    stats = scales.getStats()['cassandra']

    cass_stats['connection_errors'] = stats['connection_errors']
    cass_stats['write_timeouts'] = stats['write_timeouts']
    cass_stats['read_timeouts'] = stats['read_timeouts']
    cass_stats['other_errors'] = stats['other_errors']
    cass_stats['retries'] = stats['retries']
    cass_stats['request_timer'] = stats['request_timer']  # dict

    output_metrics_q.put((cass_stats, start, end))

    cluster.shutdown()


if __name__ == "__main__":
    benchmark()