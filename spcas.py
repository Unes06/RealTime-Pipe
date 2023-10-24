from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create a SparkSession
spark = SparkSession.builder.appName("KafkaStreamProcessing").getOrCreate()

# Add the Cassandra connector library
spark.sparkContext.addPyFile("https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.1.0/spark-cassandra-connector_2.12-3.1.0.jar")

# Define the schema to match the JSON structure
schema = StructType([
    StructField("full_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("location", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("postcode", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("email", StringType(), True)
])

# Read data from Kafka as a streaming DataFrame
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19093,kafka3:19094") \
    .option("subscribe", "random_names") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the JSON data
result_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema).alias("data")) \
    .select("data.*")

# Group by 'id' and calculate cumulative average of 'price'
# result_df = parsed_df.groupBy("id") \
#     .agg({"price": "avg"}) \
#     .withColumnRenamed("avg(price)", "cumulative_avg_price")

# Function to write data to Cassandra
def write_to_cassandra(df, epoch_id):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="random_names", keyspace="spark_streaming") \
        .mode("append") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .save()

# Write data to Cassandra using the foreach sink
query = result_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_cassandra) \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()