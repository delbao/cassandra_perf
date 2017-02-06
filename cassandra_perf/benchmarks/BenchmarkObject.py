import traceback
from cProfile import Profile
import random

TABLE = 'campaign_analytics_hourly_offset'
TOTAL_KAFKA_PARTITION = 5


class BenchmarkObject:

    def __init__(self, process_num, kwargs, hosts,
                 num_queries, num_keys, profile, output_metrics_q):
        self.process_num = process_num
        self.hosts = hosts
        self.kwargs = kwargs
        self.num_queries = num_queries
        self.num_keys = num_keys  # to scale
        self.profiler = Profile() if profile else None
        self.output_metrics_q = output_metrics_q

    def start_profile(self):
        if self.profiler:
            self.profiler.enable()

    def finish_profile(self):
        if self.profiler:
            self.profiler.disable()
            self.profiler.dump_stats('profile-%d' % self.process_num)

    # TODO: this is a overridable function
    def run_query(self, session):
        """ run the read-modify-write query to update counter
        :param key:
        :return: a future to wait for result
        """

        # randomly generate a key between range
        # for simiplicity, just assume 200 campaign_id,
        # (cid, hour) is the key, so just let hour be tied to process number
        campaign_id = random.randint(0, self.num_keys-1)
        hour = self.process_num

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
            try:
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
            except IndexError:
                print campaign_id, hour
            return session.execute_async(lwt_query)
        except Exception:
            print("Exception in worker:")
            traceback.print_exc()
            raise
