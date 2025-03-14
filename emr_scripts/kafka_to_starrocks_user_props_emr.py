from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from typing import TypeVar
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import json
from pyspark.sql.types import StructType, StringType,  TimestampType, MapType, IntegerType, LongType, BooleanType

DECODER_MAPPING = {
    "gANOLg==": None,  # pickle-protocol=3
    "gAOILg==": True,  # pickle-protocol=3
    "gAOJLg==": False,  # pickle-protocol=3
    "gAROLg==": None,  # pickle-protocol=4
    "gASILg==": True,  # pickle-protocol=4
    "gASJLg==": False,  # pickle-protocol=4
}
T = TypeVar("T")


def decode_values(value: T) -> T:
  """
  Aerospike pickles and encodes None/True/False values.
  Recursively find these encoded values and decode them.
  """
  if isinstance(value, dict):
    return {k: decode_values(v) for k, v in value.items()}
  elif isinstance(value, list):
    return [decode_values(v) for v in value]
  elif isinstance(value, tuple):
    return tuple(decode_values(v) for v in value)
  elif isinstance(value, str):
    if value in DECODER_MAPPING:
      return DECODER_MAPPING[value]
    else:
      try:
        return json.dumps(decode_values(json.loads(value)))
      except Exception:
        return value
  else:
    return value


serialize_udf = udf(decode_values, StringType())

# Define the schema of the incoming data
schema = (StructType() \
    .add("metadata", StringType()) \
    .add("app_id", StringType()) \
    .add("bsin", StringType()) \
    .add("email", StringType()) \
    .add("app_member_id", StringType()) \
    .add("sub_site_ids", StringType()) \
    .add("replaced_by", StringType()) \
    .add("created_at", LongType()) \
    .add("last_updated", LongType()) \
    .add("imported_from", StringType()) \
    .add("consent", StringType()) \
    .add("external_ids", StringType()) \
    .add("unique_cli_ids", StringType()) \
    .add("properties", StringType()) \
    .add("s_attributes", StringType()) \
    .add("contacts", StringType()) \
    .add("s_contacts", StringType()) \
    # .add("merged_bsins", StringType()) \
          )

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaUserPropsLoader") \
    .getOrCreate()

# Kafka parameters
kafka_bootstrap_servers = "b-1.preprod-msk-asconnect.zmi1o9.c3.kafka.us-east-1.amazonaws.com:9092,b-2.preprod-msk-asconnect.zmi1o9.c3.kafka.us-east-1.amazonaws.com:9092,b-3.preprod-msk-asconnect.zmi1o9.c3.kafka.us-east-1.amazonaws.com:9092"  # Kafka server address
kafka_topic = "as-identity-bsin"  # Replace with your Kafka topic name

# Read the stream from Kafka topic

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("failOnDataLoss","false") \
    .load()


# take the value which is a json and create df with json keys as columns

df = df.select(from_json(col("value").cast("string"), schema).alias("value"))
# Convert dataframe where each column is a key in the json to a dataframe where the json keys are columns
df = df.selectExpr(
"'boomtrain' as site_id",
  "value.bsin as bsin",
  "value.app_member_id as user_id",
  "value.email as email",
  "value.contacts as contacts",
  "value.properties as properties",
  "CAST(CAST(value.last_updated AS TIMESTAMP) AS STRING) as last_updated",
  "value.replaced_by as replaced_by",
  "md5(value.email) as email_md5 ",
  "value.sub_site_ids as sub_site_ids",
  "value.s_attributes as scoped_properties",
  "value.s_contacts as scoped_contacts",
  "value.imported_from as imported_from",
  # "value.merged_bsins as merged_bsins",
  "'{}' as merged_bsins",
  "value.consent as consent",
  "value.external_ids as external_ids",
  "value.unique_cli_ids as unique_client_ids"
)


for col_name in df.columns:
    df = df.withColumn(col_name + "_serialized", serialize_udf(df[col_name]))
    df = df.drop(col_name)
    df = df.withColumnRenamed(col_name + "_serialized", col_name)

# print(f"Number of rows written to DB {df.count()}")

# 2. Write to StarRocks by configuring the format as "starrocks" and the following options.
# You need to modify the options according your own environment.

df.writeStream.format("starrocks") \
    .outputMode("append") \
    .option("starrocks.fe.http.url", "10.170.46.131:8030") \
    .option("starrocks.fe.jdbc.url", "jdbc:mysql://26e04xo1x-internal.cloud-app.celerdata.com:9030") \
    .option("starrocks.table.identifier", "CRM.user_props") \
    .option("starrocks.user", "admin") \
    .option("starrocks.password", "R4nd0mPa$$") \
    .option("checkpointLocation","/tmp/checkpoints/") \
    .start() \
    .awaitTermination()

# # You can also perform further transformations on the data as needed
# # Example: show the first few records
# df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start() \
#     .awaitTermination()
