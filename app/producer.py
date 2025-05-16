import json
import requests
import sseclient
from kafka import KafkaProducer

TOPIC_NAME = "input_stream"
BOOTSTRAP_SERVER = "kafka-server:9092"
WIKI_URL = "https://stream.wikimedia.org/v2/stream/page-create"

def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    client = sseclient.SSEClient(WIKI_URL)

    print(f"Start streaming messages to topic: {TOPIC_NAME}", flush=True)
    for event in client:
        if event.event == 'message':
            try:
                data = json.loads(event.data)
                producer.send(TOPIC_NAME, value=data)
                print(
                    f"---[{data['meta']['dt']}] Sent: {data['meta']['request_id']}", flush=True)
            except Exception as e:
                print(f"Skipped event: {e}", flush=True)



if __name__ == "__main__":
    main()
