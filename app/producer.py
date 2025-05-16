import json
import requests
import sseclient
from kafka import KafkaProducer


def main():
    producer = KafkaProducer(
        bootstrap_servers="kafka-server:9092",
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    wiki_url = "https://stream.wikimedia.org/v2/stream/page-create"
    topic = "input_stream"

    client = sseclient.SSEClient(wiki_url)

    print(f"Start streaming messages to topic: {topic}")
    for event in client:
        if event.event == 'message':
            try:
                data = json.loads(event.data)
                producer.send(topic, value=data)
                print(
                    f"---[{data['meta']['dt']}] Sent: {data['meta']['request_id']}")
            except Exception as e:
                print(f"Skipped event: {e}")



if __name__ == "__main__":
    main()