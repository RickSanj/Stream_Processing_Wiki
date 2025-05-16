from pyspark.sql import SparkSession
from pyspark.sql.streaming import *
from pyspark.sql.functions import *


spark = SparkSession \
    .builder \
    .appName("Spark Kafka Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .config("spark.sql.streaming.checkpointLocation", "/opt/app/spark-checkpoint") \
    .getOrCreate()


kafka_bootstrap_servers = "kafka-server:9092"
input_topic_name = "input_stream"
output_topic_name = "output_stream"


df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_topic_name) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .select(col("value").cast("string").alias("value"))

df_processed = df \
    .withColumn("Message_length", length(col("value"))) \
    .withColumn("Time_of_processing", current_timestamp()) \
    .withColumn("value", concat_ws(
        "; ",
        concat_ws(": ", lit("Message_length"), col("Message_length")),
        concat_ws(": ", lit("Time of processing"), col("Time_of_processing")),
        concat_ws(": ", lit("Original message"), col("value"))
    ))

df_processed.select(
    lit(None).cast("string").alias("key"),
    col("value").cast("string")
).writeStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", output_topic_name) \
    .option("checkpointLocation", "/opt/app/kafka_checkpoint").start().awaitTermination()
