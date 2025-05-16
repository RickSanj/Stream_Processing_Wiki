docker run --rm -it --network hw11-network \
  -v ./app:/opt/app \
  bitnami/spark:3 \
  spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 \
  /opt/app/main.py
