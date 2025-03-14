from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json

from pyspark.sql.types import StructType, StringType,  TimestampType, MapType, IntegerType, LongType, BooleanType

identity = (StructType() \
    .add("bsin", StringType())\
    .add("user_id", StringType()) \
    .add("email", StringType()))

# Define the schema of the incoming data
schema = (StructType() \
    .add("site_id", StringType()) \
    .add("bsin", StringType()) \
    .add("event_id", StringType()) \
    .add("event_type", StringType()) \
    .add("identified", StringType()) \
    .add("resource_id", StringType()) \
    .add("resource_type", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("url", StringType()) \
    .add("identity", identity) \
    .add("metadata", StringType()) \
    .add("properties", StringType()) \
    .add("property_format", StringType()) \
    .add("enriched", StringType()) \
    .add("status", StringType()) \
    .add("source_event_id", StringType()) \
    .add("is_test", BooleanType())
          )

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkExample") \
    .getOrCreate()

# Kafka parameters
kafka_bootstrap_servers = "b-1.preprodmskevents2.w7ke8n.c17.kafka.us-east-1.amazonaws.com:9092,b-2.preprodmskevents2.w7ke8n.c17.kafka.us-east-1.amazonaws.com:9092,b-3.preprodmskevents2.w7ke8n.c17.kafka.us-east-1.amazonaws.com:9092"  # Kafka server address
kafka_topic = "events_processed"  # Replace with your Kafka topic name

# Read the stream from Kafka topic

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("failOnDataLoss","false") \
    .load()

# The value column from Kafka is in binary format, so we need to cast it to a string
# df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# take the value which is a json and create df with json keys as columns

df = df.select(from_json(col("value").cast("string"), schema).alias("value"))
# Convert dataframe where each column is a key in the json to a dataframe where the json keys are columns
df = df.selectExpr(
  "value.event_id as event_id",
  "value.event_type as event_type",
  "'boomtrain' as site_id",
  "value.identity.bsin as bsin",
  "value.identity.user_id as user_id",
  "value.identity.email as email",
  "value.properties as properties",
  "value.timestamp  as dt",
  "current_timestamp() as etl_time")


# 2. Write to StarRocks by configuring the format as "starrocks" and the following options.
# You need to modify the options according your own environment.
df.writeStream.format("starrocks") \
    .outputMode("append") \
    .option("starrocks.fe.http.url", "10.170.46.131:8030") \
    .option("starrocks.fe.jdbc.url", "jdbc:mysql://26e04xo1x-internal.cloud-app.celerdata.com:9030") \
    .option("starrocks.table.identifier", "CRM.EVENTS_boomtrain") \
    .option("starrocks.user", "admin") \
    .option("starrocks.password", "R4nd0mPa$$") \
    .option("starrocks.column.types","event_time timestamp") \
    .option("checkpointLocation","/tmp/checkpoints/") \
    .start() \
    .awaitTermination()
#
# # You can also perform further transformations on the data as needed
# # Example: show the first few records
# df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start() \
#     .awaitTermination()
