import time
from cassandra.cluster import Cluster
import json


class CassandraClient:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None

    def connect(self):
        retries = 10
        for i in range(retries):
            try:
                cluster = Cluster([self.host], port=self.port)
                self.session = cluster.connect(self.keyspace)
                self.session.set_keyspace(self.keyspace)
                print("Connected to Cassandra!")
                return
            except Exception as e:
                print(
                    f"Retrying Cassandra connection ({i+1}/{retries})... {e}")
                time.sleep(5)
        raise Exception(
            "Failed to connect to Cassandra after multiple attempts.")

    def execute(self, query):
        self.session.execute(query)

    def close(self):
        self.session.shutdown()

    def read_from_table(self, query):
        return self.session.execute(query)

    def insert_record(self, data):
        event_time = data['event_time']
        domain = data['domain']
        user_name = data['user_name']
        user_id = data['user_id']
        user_is_bot = bool(data['user_is_bot'])
        page_id = data['page_id']
        page_title = data['page_title']

        # Insert into domain table (partitioned by domain)
        self.session.execute("""
            INSERT INTO domain (time, domain, page_id, user_is_bot)
            VALUES (%s, %s, %s, %s)
        """, (event_time, domain, page_id, user_is_bot))

        # Insert into hour table (partitioned by time)
        self.session.execute("""
            INSERT INTO hour (time, domain, page_id, user_is_bot)
            VALUES (%s, %s, %s, %s)
        """, (event_time, domain, page_id, user_is_bot))

        # Insert/update into users table (overwrites entire row since user_id is the PK)
        self.session.execute("""
            INSERT INTO users (user_id, user_name, time, page_id, page_title)
            VALUES (%s, %s, %s, %s, %s)
        """, (user_id, user_name, event_time, page_id, page_title))

        # Insert into page_by_id table
        self.session.execute("""
            INSERT INTO page_by_id (page_id, page_title, domain)
            VALUES (%s, %s, %s)
        """, (page_id, page_title, domain))

    def insert_agg_record(self, last_hour, new_hour, map_all_users, map_bots_only):
        # Insert all users data
        self.session.execute("""
            INSERT INTO hourly_domain_stats (time_start, time_end, statistics, bots_only)
            VALUES (%s, %s, %s, %s)
        """, (last_hour, new_hour, map_all_users, False))

        # Insert bots-only data
        self.session.execute("""
            INSERT INTO hourly_domain_stats (time_start, time_end, statistics, bots_only)
            VALUES (%s, %s, %s, %s)
        """, (last_hour, new_hour, map_bots_only, True))
