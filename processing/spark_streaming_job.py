import findspark
findspark.init()

import logging
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, lower, trim, when,from_unixtime

# Clean up any existing handlers
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# --- Setup Logging ---
log_file = 'quickcommerce_stream.log'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Load config file
config = configparser.ConfigParser()
config.read('conn.txt')

# Read Snowflake configs
sf_cfg = config['snowflake']

   
# Initialize Spark session with Snowflake connector
spark = SparkSession.builder \
    .appName("Stream data direct Load to Snowflake") \
    .config("spark.jars.packages",
            "net.snowflake:snowflake-jdbc:3.13.14,"
            "net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1") \
    .getOrCreate()

# Load raw data
gps_df = spark.read.option("header", "true").csv("file:///C:/Users/rajeshitha/Desktop/Sathya/Quick_Commerce/Final/gps_streams.csv")
status_df = spark.read.option("header", "true").csv("file:///C:/Users/rajeshitha/Desktop/Sathya/Quick_Commerce/Final/status_streams.csv")

logger.info("Gps data loaded to dataframe gps_df")
print("Gps data loaded to dataframe gps_df")
gps_df.show(5)
logger.info("Status data loaded to dataframe status_df")
print("Status data loaded to dataframe status_df")
status_df.show(5)


# Clean GPS data
gps_cleaned = gps_df \
    .withColumn("lat", col("lat").cast("double")) \
    .withColumn("lon", col("lon").cast("double")) \
    .withColumn("ts", from_unixtime(col("ts"))) \
    .withColumn("ts", col("ts").cast("timestamp")) \
    .withColumn("courier_id", lower(col("courier_id")))\
    .filter((col("lat").isNotNull()) & (col("lon").isNotNull()))

logger.info("Datatypes corected for GPS data")
print("Datatypes corected for GPS data")
gps_cleaned.printSchema()
logger.info("Null values removed from GPS data")
print("Null values removed from GPS data")
logger.info("Timestamp converted to datetime")
print("Timestamp converted to datetime")
logger.info("Courier id converted to lowercase")
print("Courier id converted to lowercase")

logger.info("Gps data cleaned")
print("Gps data cleaned")
gps_cleaned.show(5)

# Clean Status data
status_cleaned = status_df \
    .withColumn("ts", from_unixtime(col("ts"))) \
    .withColumn("ts", col("ts").cast("timestamp")) \
    .withColumn("status", lower(col("status"))) \
    .withColumn("order_id", col("order_id").cast("int")) \
    .withColumn("courier_id", lower(col("courier_id")))

logger.info("Datatypes corected for Status data")
print("Datatypes corected for Status data")
status_cleaned.printSchema()
logger.info("Timestamp converted to datetime")
print("Timestamp converted to datetime")
logger.info("Status converted to lowercase")
print("Status converted to lowercase")

# Snowflake connection
sf_opts = {
    "sfURL": sf_cfg['sfURL'],
    "sfUser": sf_cfg['sfUser'],
    "sfPassword": sf_cfg['sfPassword'],
    "sfDatabase": sf_cfg['sfDatabase'],
    "sfSchema": sf_cfg['sfSchema'],
    "sfWarehouse": sf_cfg['sfWarehouse']
}

# Write GPS data to Snowflake
gps_cleaned.write.format("snowflake") \
    .options(**sf_opts) \
    .option("dbtable", "gps_dim") \
    .mode("overwrite") \
    .save()

# Write Status data to Snowflake
status_cleaned.write.format("snowflake") \
    .options(**sf_opts) \
    .option("dbtable", "status_dim") \
    .mode("overwrite") \
    .save()

logger.info("Data loaded to Snowflake")
print("Data loaded to Snowflake")

spark.stop()

