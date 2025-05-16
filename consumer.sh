docker run -it --rm \
 --network hw11-network \
 bitnami/kafka:latest kafka-console-consumer.sh \
 --bootstrap-server kafka-server:9092 \
 --topic output_stream