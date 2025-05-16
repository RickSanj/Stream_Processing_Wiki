from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.streaming import *
from pyspark.sql.functions import *
from pyspark.sql.types import *


schema = StructType([
    StructField("meta", StructType([
        StructField("dt", StringType()),
        StructField("domain", StringType())
    ])),
    StructField("performer", StructType([
        StructField("user_text", StringType()),
        StructField("user_id", LongType()),
        StructField("user_is_bot", BooleanType())
    ])),
    StructField("page_id", LongType()),
    StructField("page_title", StringType()),
])

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


df_json = df \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.meta.dt").alias("event_time_str"),
        to_timestamp("data.meta.dt").alias("event_time"),
        col("data.meta.domain").alias("domain"),
        col("data.performer.user_text").alias("user_name"),
        col("data.performer.user_id").alias("user_id"),
        col("data.performer.user_is_bot").alias("user_is_bot"),
        col("data.page_id").alias("page_id"),
        col("data.page_title").alias("page_title")
    )


df_json \
    .select(
        lit(None).cast("string").alias("key"),
        to_json(struct("*")).alias("value")
    ).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", output_topic_name) \
    .option("checkpointLocation", "/opt/app/kafka_checkpoint/") \
    .start() \
    .awaitTermination()
