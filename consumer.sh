docker run -it --rm \
 --network proj-network \
 bitnami/kafka:latest kafka-console-consumer.sh \
 --bootstrap-server kafka-server:9092 \
 --topic output_stream