import findspark
findspark.init()

import logging
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, lower, trim, when

# Clean up any existing handlers
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# --- Setup Logging ---
log_file = 'quickcommerce.log'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Load config file
config = configparser.ConfigParser()
config.read('conn.txt')

# Read MySQL and Snowflake configs
mysql_cfg = config['mysql']
sf_cfg = config['snowflake']

# Creating a Spark session
spark = SparkSession.builder \
    .appName("Data Pipeline for Quick Commerce Apps") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0,"
            "net.snowflake:snowflake-jdbc:3.13.14,"
            "net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1") \
    .config("spark.driver.extraClassPath", r"C:\Users\rajeshitha\Downloads\mysql-connector-j-9.2.0\mysql-connector-j-9.2.0.jar") \
    .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017") \
    .config("spark.mongodb.read.database", "quick_commerce") \
    .config("spark.mongodb.read.collection", "inventory") \
    .getOrCreate()

# MySQL JDBC connection
jdbc_url = f"jdbc:mysql://{mysql_cfg['db_host']}:3306/{mysql_cfg['db_name']}"
jdbc_props = {
    "user": mysql_cfg['db_user'],
    "password": mysql_cfg['db_password'],
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Load MySQL orders table
order_df = spark.read.jdbc(url=jdbc_url, table="orders", properties=jdbc_props)
order_count = order_df.count()
# Show sample orders data
order_df.show(5)
logger.info(f"Order data loaded successfully with {order_count} records")
print(f"Order data loaded successfully with {order_count} records")

# Load MongoDB inventory data
inventory_df = spark.read.format("mongodb").load()
inventory_count = inventory_df.count()

expected_order = ['item_id', 'name', 'category', 'Warehouse', 'stock_level', 'last_update']
inventory_df = inventory_df.select(*expected_order)
inventory_df.show(5)
logger.info(f"Inventory data loaded successfully with {inventory_count} records")
print(f"Inventory data loaded successfully with {inventory_count} records")

################# Data Cleaning and Transformation#####################
# Checking for nulls or missing values and handling them.

# Handle missing or null values by either dropping or filling them.
from functools import reduce

# Identifying the null values
order_df_with_missing_values = order_df.filter(
    reduce(lambda a, b: a | b, [col(c).isNull() for c in order_df.columns])
)
order_df_with_missing_values.show()

inventory_df_with_missing_values = inventory_df.filter(
    reduce(lambda a, b: a | b, [col(c).isNull() for c in inventory_df.columns])
)
inventory_df_with_missing_values.show()

logging.info("Null values identified successfully")
print("Null values identified successfully")

# Dropping the null rows

order_df=order_df.dropna()
inventory_df=inventory_df.dropna()

logging.info("Null values removed successfully")
print("Null values removed successfully")

# Filling the missing values with dafault

order_df=order_df.fillna({
    "quantity":0,
    "price":0.0,
    "status":"unknown"
})
inventory_df=inventory_df.fillna({
    "category":"unknown",
    "warehouse":"unknown",
    "stock_level":0
})

logging.info("Missing values filled with default values successfully")
print("Missing values filled with default values successfully")

# Convert timestamps into consistent formats for joining and filtering.
# Converting timestamps to datetime format

# Correcting data types (e.g., ensuring Order_ts is in Datetime , Quantity is an integer, price is in double etc.).

order_df.printSchema()

inventory_df=inventory_df.withColumn("stock_level", col("stock_level").cast("int"))
inventory_df=inventory_df.withColumn("item_id", col("item_id").cast("int"))
inventory_df=inventory_df.withColumn("last_update", col("last_update").cast("timestamp"))

inventory_df.printSchema()

logging.info("Data types corrected successfully")
print("Data types corrected successfully")

#  Standardize string fields (e.g., lowercase, trim spaces).
order_df = order_df.withColumn("status", lower(trim(col("status"))))
inventory_df = inventory_df.withColumn("name", lower(trim(col("name"))))
inventory_df = inventory_df.withColumn("category", lower(trim(col("category"))))
inventory_df = inventory_df.withColumn("warehouse", lower(trim(col("warehouse"))))
logging.info("String fields standardized successfully")
print("String fields standardized successfully")


# Removing duplicates
order_df = order_df.dropDuplicates()
inventory_df = inventory_df.dropDuplicates()
logging.info("Duplicates removed successfully")
print("Duplicates removed successfully")
# Removing test entries like status = test

order_df = order_df.filter(col("status") != "test")
logging.info("Test entries removed successfully")
print("Test entries removed successfully")


# Renaming the column names to snake-case for consistency

order_df = order_df.select([col(c).alias(c.lower().replace(' ', '_')) for c in order_df.columns])
order_df.show(5)

inventory_df=inventory_df.select([col(c).alias(c.lower().replace(' ', '_')) for c in inventory_df.columns])
inventory_df.show(5)

logging.info("Column names renamed to snake-case for consistency successfully")
print("Column names renamed to snake-case for consistency successfully")


# Calculate total order value (quantity * price)
order_df = order_df.withColumn("total_order_value",round((col("quantity") * col("price")),2))
order_df.show(5)

logging.info("Total order value calculated successfully")
print("Total order value calculated successfully")

# Normalize status values (e.g., delivered, cancelled, pending)

order_df = order_df.withColumn("status",
    when(col("status").like("%deliv%"), "delivered")
    .when(col("status").like("%cancel%"), "cancelled")
    .when(col("status").like("%process%"), "processing")
    .when(col("status").like("%pending%"), "pending")
    .when(col("status").like("%ship%"), "shipped")
    .otherwise(col("status"))
)

logging.info("Status values normalized successfully")
print("Status values normalized successfully")


# Handle missing stock levels (fill with 0 or flag for alerts)
inventory_df=inventory_df.withColumn("stock_flag",
                                     when(col("stock_level")==0,"Out of stock")
                                     .when(col("stock_level")<20,"Low stock")
                                     .otherwise("In stock")
                                     )
inventory_df.show(5)

inventory_stock=inventory_df.filter((col("stock_flag")== "Out of stock") | (col("stock_flag")=="Low stock"))
a=inventory_stock.count()
print(f"There are {a} items with low stock or out of stock")
inventory_out_of_stock=inventory_stock.filter(col("stock_flag")=="Out of stock")
b=inventory_out_of_stock.count()
print(f"There are {b} items out of stock")
inventory_low_stock=inventory_stock.filter(col("stock_flag")=="Low stock")
c=inventory_low_stock.count()
print(f"There are {c} items with low stock")

logging.info("Missing stock levels handled successfully")
print("Missing stock levels handled successfully")

# Flattening the nested json
inventory_df.printSchema();
logging.info("Inventory json has no nested json")
print("Inventory json has no nested json")

# Create order_date
order_df = order_df.withColumn("order_date", col("order_ts").cast("date"))
logger.info("order_date column created")
print("order_date column created")

# Snowflake connection
sf_opts = {
    "sfURL": sf_cfg['sfURL'],
    "sfUser": sf_cfg['sfUser'],
    "sfPassword": sf_cfg['sfPassword'],
    "sfDatabase": sf_cfg['sfDatabase'],
    "sfSchema": sf_cfg['sfSchema'],
    "sfWarehouse": sf_cfg['sfWarehouse']
}

# Write to Snowflake
order_df.write.format("snowflake") \
    .options(**sf_opts) \
    .option("dbtable", "orders_dim") \
    .mode("overwrite") \
    .save()

inventory_df.write.format("snowflake") \
    .options(**sf_opts) \
    .option("dbtable", "inventory_dim") \
    .mode("overwrite") \
    .save()

logger.info("Data written to Snowflake")
print("Data pipeline executed successfully!")

spark.stop()
