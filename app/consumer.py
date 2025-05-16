from kafka import KafkaConsumer
from cassandra_client import CassandraClient
import json
from datetime import datetime


TOPIC_NAME = "output_stream"
BOOTSTRAP_SERVER = 'kafka-server:9092'
OUTPUT_DIR = "/usr/src/app/output"
CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = 9042
KEYSPACE = "wiki_analytics"


def main():
    client = CassandraClient(CASSANDRA_HOST, CASSANDRA_PORT, KEYSPACE)
    client.connect()

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

    print("Listening for wiki events...")
    map_all_users = {}
    map_bots_only = {}
    last_hour_timestamp = None
    try:
        while True:
            for message in consumer:
                data = message.value

                event_time = datetime.strptime(
                    data['event_time'], '%Y-%m-%dT%H:%M:%S.%fZ')
                new_hour_timestamp = event_time.replace(
                    minute=0, second=0, microsecond=0)

                domain = data['domain']
                user_is_bot = data["user_is_bot"]

                print(f"Consumed: {data}")
                client.insert_record(data)

                # when new hour starts data about previous hour should be inserted
                if last_hour_timestamp is None:
                    last_hour_timestamp = new_hour_timestamp
                if new_hour_timestamp != last_hour_timestamp:
                    client.insert_agg_record(
                        last_hour_timestamp, new_hour_timestamp, map_all_users, map_bots_only)
                    map_all_users = {}
                    map_bots_only = {}
                    last_hour_timestamp = new_hour_timestamp
                try:
                    map_all_users[domain] += 1
                except KeyError:
                    map_all_users[domain] = 1
                if user_is_bot:
                    try:
                        map_bots_only[domain] += 1
                    except KeyError:
                        map_bots_only[domain] = 1

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
