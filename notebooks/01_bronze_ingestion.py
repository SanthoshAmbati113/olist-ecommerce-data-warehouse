# Databricks notebook source
# Select the project catalog and schema

spark.sql("USE CATALOG `e-commerce`")
spark.sql("USE SCHEMA ecommerce_bronze")

# Define the source directory
SOURCE_PATH = "/Volumes/e-commerce/ecommerce_bronze/raw_files/archive (25)/"

# Check uploaded files
display(dbutils.fs.ls(SOURCE_PATH))

# COMMAND ----------

file_table_mapping = {
    "olist_customers_dataset.csv": "bronze_customers",
    "olist_orders_dataset.csv": "bronze_orders",
    "olist_order_items_dataset.csv": "bronze_order_items",
    "olist_order_payments_dataset.csv": "bronze_payments",
    "olist_order_reviews_dataset.csv": "bronze_reviews",
    "olist_products_dataset.csv": "bronze_products",
    "olist_sellers_dataset.csv": "bronze_sellers",
    "olist_geolocation_dataset.csv": "bronze_geolocation",
    "product_category_name_translation.csv": "bronze_category_translation"
}

# COMMAND ----------

from pyspark.sql import functions as F

# Select Bronze schema
spark.sql("USE CATALOG `e-commerce`")
spark.sql("USE SCHEMA ecommerce_bronze")

# Ingestion function
def ingest_to_bronze(file_name, table_name):

    file_path = SOURCE_PATH + file_name

    print(f"Processing: {file_name}")

    # Read CSV
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .load(file_path)
    )

    # Add ingestion metadata
    df = (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    # Save as Delta table
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    print(f"Successfully created: {table_name}")

# Ingest all 9 datasets
for file_name, table_name in file_table_mapping.items():

    ingest_to_bronze(file_name, table_name)

print("BRONZE INGESTION COMPLETED!")

# COMMAND ----------

tables = spark.sql("""
    SHOW TABLES IN `e-commerce`.ecommerce_bronze
""")

display(tables)

# COMMAND ----------

for table_name in file_table_mapping.values():

    df = spark.table(table_name)

    print(
        f"{table_name}: {df.count()} records"
    )

# COMMAND ----------

display(
    spark.table("bronze_orders")
)