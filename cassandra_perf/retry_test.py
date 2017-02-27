#
import cassandra
from cassandra.cluster import Cluster
from cassandra.io.asyncorereactor import AsyncoreConnection

import logging

log = logging.getLogger('retry_test')
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

logging.getLogger('retry_test').setLevel(logging.DEBUG)

KEYSPACE = 'dbao_ad_analytics_realtime_retry'
TABLE = 'campaign_analytics_hourly_offset'

COLUMNS_MAP = {
    'campaign_id': 'int',
    'hour': 'int',
    'clicks_count': 'int',
    'kafka_partition': 'int',
    'kafka_offset': 'int',
}

PRIMARY_KEY = "(campaign_id, hour, kafka_partition)"

# ad_backend_dev
# hosts = ["10.40.10.232","10.40.10.153","10.40.22.145","10.40.22.146"]

# distsys-test-dev
hosts = ["10.40.11.250", "10.40.11.237", "10.40.23.109", "10.40.22.223",
         "10.40.11.0"]

# create keyspace
log.info("Using 'cassandra' package from %s", cassandra.__path__)

cluster = Cluster(hosts, schema_metadata_enabled=False,
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

    # log.info("Inserting {} keys ... ".format(args.num_keys))

    # insert one row
    cols = "(campaign_id, hour, kafka_partition, clicks_count, " \
           "kafka_offset)"

    prepared_query = "INSERT INTO {table} {cols} VALUES (?,?,?,?,?)".format(
        table=TABLE,
        cols=cols)
    prepared = session.prepare(prepared_query)

    campaign_id = 0
    for hour in range(2):
        for kafka_partition in range(2):
            session.execute(prepared.bind((campaign_id, hour,
                                           kafka_partition, 0, 0)))

    cond = "campaign_id={cid} and hour={hour} and kafka_partition={" \
           "kafka_partition}".format(cid=campaign_id,
                                     hour=hour,
                                     kafka_partition=kafka_partition)

    # read file == no, just a list of offsets
    # list of offset
    offset_list = [0, 1, 2, 3, 4, 4, 5, 5, 3, 4, 5, 6, 5, 6, 7]
    count_list = [5] * len(offset_list)

    # just run lwt with about list
    # run lwt query to update count
    new_count = 0
    for i in range(len(offset_list)):
        try:
            new_offset = offset_list[i]
            new_count += count_list[i]
            lwt_query = "UPDATE {table} SET kafka_offset={new_offset}, " \
                        "clicks_count={new_count} " \
                        "WHERE {cond} " \
                        "if kafka_offset<{new_offset}".format(
                table=TABLE,
                new_count=new_count,
                new_offset=new_offset,
                cond=cond)

            # execute
            result = session.execute(lwt_query)
            log.info("execute lwt with {offset}, result={result}".format(
                offset=offset_list[i], result=result.was_applied))
            if not result.was_applied:
                new_count -= count_list[i]
        except IndexError:
            print campaign_id, hour

finally:
    cluster.shutdown()

    # done. verify: max  count = 5 * offset
