# Databricks notebook source
# DBTITLE 1,Phase 6 environment setup
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

import time

CATALOG = "e-commerce"
SCHEMA = "ecommerce_gold"

FACT_TABLE = f"{CATALOG}.{SCHEMA}.fact_order_items"
PRODUCT_TABLE = f"{CATALOG}.{SCHEMA}.dim_products"

MERGE_TABLE = f"{CATALOG}.{SCHEMA}.merge_demo"
MERGE_SOURCE = f"{CATALOG}.{SCHEMA}.merge_source"

print("Phase 6 environment initialized")

# COMMAND ----------

fact = spark.table(FACT_TABLE)
products = spark.table(PRODUCT_TABLE)

print("Fact table schema:")
fact.printSchema()

print("Product dimension schema:")
products.printSchema()

print("Fact partitions:", fact.rdd.getNumPartitions())
print("Product partitions:", products.rdd.getNumPartitions())

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {FACT_TABLE}"))
display(spark.sql(f"DESCRIBE DETAIL {PRODUCT_TABLE}"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `e-commerce`.ecommerce_gold.merge_demo
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT 1 AS customer_id, 'Santhosh' AS customer_name, 100 AS total_orders
# MAGIC UNION ALL
# MAGIC SELECT 2, 'Rahul', 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `e-commerce`.ecommerce_gold.merge_source
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT 1 AS customer_id, 'Santhosh' AS customer_name, 120 AS total_orders
# MAGIC UNION ALL
# MAGIC SELECT 3, 'Kiran', 30;

# COMMAND ----------

# MAGIC %md
# MAGIC Execute MERGE

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO `e-commerce`.ecommerce_gold.merge_demo AS target
# MAGIC
# MAGIC USING `e-commerce`.ecommerce_gold.merge_source AS source
# MAGIC
# MAGIC ON target.customer_id = source.customer_id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET
# MAGIC         target.customer_name = source.customer_name,
# MAGIC         target.total_orders = source.total_orders
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         customer_id,
# MAGIC         customer_name,
# MAGIC         total_orders
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.customer_id,
# MAGIC         source.customer_name,
# MAGIC         source.total_orders
# MAGIC     );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM `e-commerce`.ecommerce_gold.merge_demo
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake Time Travel

# COMMAND ----------

history_df = DeltaTable.forName(
    spark,
    MERGE_TABLE
).history()

display(history_df)

# COMMAND ----------

initial_version = (
    history_df
    .agg(F.max("version").alias("version"))
    .first()["version"]
)

print("Initial version:", initial_version)

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE `e-commerce`.ecommerce_gold.merge_demo
# MAGIC SET total_orders = 999
# MAGIC WHERE customer_id = 1;

# COMMAND ----------

latest_df = spark.table(MERGE_TABLE)

display(latest_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC old_df = (
# MAGIC     spark.read
# MAGIC     .format("delta")
# MAGIC     .option("versionAsOf", initial_version)
# MAGIC     .load(
# MAGIC         "/Volumes/e-commerce/ecommerce_bronze/raw_files/archive/"
# MAGIC     )
# MAGIC )

# COMMAND ----------

old_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", initial_version)
    .table(MERGE_TABLE)
)

display(old_df)

# COMMAND ----------

display(
    DeltaTable.forName(spark, MERGE_TABLE).history()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Schema evolution

# COMMAND ----------

initial_data = [
    (1, "Santhosh", 100),
    (2, "Rahul", 50)
]

initial_df = spark.createDataFrame(
    initial_data,
    ["customer_id", "customer_name", "total_orders"]
)

SCHEMA_TABLE = f"{CATALOG}.{SCHEMA}.schema_evolution_demo"

(
    initial_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SCHEMA_TABLE)
)

# COMMAND ----------

new_data = [
    (3, "Kiran", 30, "Premium"),
    (4, "Priya", 80, "Regular")
]

new_df = spark.createDataFrame(
    new_data,
    [
        "customer_id",
        "customer_name",
        "total_orders",
        "customer_segment"
    ]
)

# COMMAND ----------

(
    new_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(SCHEMA_TABLE)
)

# COMMAND ----------

evolved_df = spark.table(SCHEMA_TABLE)

evolved_df.printSchema()

display(evolved_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Build a regular join

# COMMAND ----------

fact = spark.table(FACT_TABLE)
products = spark.table(PRODUCT_TABLE)

regular_join = fact.join(
    products,
    on="product_sk",
    how="inner"
)

regular_join.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC Build a broadcast join

# COMMAND ----------

from pyspark.sql.functions import broadcast

broadcast_join = fact.join(
    broadcast(products),
    on="product_sk",
    how="inner"
)

broadcast_join.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC Compare execution time

# COMMAND ----------

def measure_execution(df, label):
    start = time.perf_counter()

    result = df.agg(
        F.sum("price").alias("total_price")
    ).collect()

    elapsed = time.perf_counter() - start

    print(f"{label}: {elapsed:.3f} seconds")
    print("Result:", result)

    return elapsed


regular_time = measure_execution(
    regular_join,
    "Regular Join"
)

broadcast_time = measure_execution(
    broadcast_join,
    "Broadcast Join"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Repartition vs coalesce

# COMMAND ----------

fact = spark.table(FACT_TABLE)

print("Original partition count:",
      fact.rdd.getNumPartitions())

# COMMAND ----------

repartitioned_df = fact.repartition(16)

print(
    "After repartition:",
    repartitioned_df.rdd.getNumPartitions()
)

repartitioned_df.explain("formatted")

# COMMAND ----------

coalesced_df = fact.coalesce(4)

print(
    "After coalesce:",
    coalesced_df.rdd.getNumPartitions()
)

coalesced_df.explain("formatted")

# COMMAND ----------

REPARTITION_TABLE = (
    f"{CATALOG}.{SCHEMA}.repartition_test"
)

COALESCE_TABLE = (
    f"{CATALOG}.{SCHEMA}.coalesce_test"
)

# COMMAND ----------

start = time.perf_counter()

(
    repartitioned_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(REPARTITION_TABLE)
)

repartition_write_time = time.perf_counter() - start

print(
    "Repartition write time:",
    round(repartition_write_time, 3),
    "seconds"
)

# COMMAND ----------

start = time.perf_counter()

(
    coalesced_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(COALESCE_TABLE)
)

coalesce_write_time = time.perf_counter() - start

print(
    "Coalesce write time:",
    round(coalesce_write_time, 3),
    "seconds"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Compare output files

# COMMAND ----------

repartition_detail = spark.sql(
    f"DESCRIBE DETAIL {REPARTITION_TABLE}"
).first()

coalesce_detail = spark.sql(
    f"DESCRIBE DETAIL {COALESCE_TABLE}"
).first()

print(
    "Repartition files:",
    repartition_detail["numFiles"]
)

print(
    "Coalesce files:",
    coalesce_detail["numFiles"]
)

print(
    "Repartition size:",
    repartition_detail["sizeInBytes"]
)

print(
    "Coalesce size:",
    coalesce_detail["sizeInBytes"]
)

# COMMAND ----------

fact.printSchema()

# COMMAND ----------

PARTITIONED_TABLE = (
    f"{CATALOG}.{SCHEMA}.partitioned_order_test"
)

(
    fact.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("order_status")
    .saveAsTable(PARTITIONED_TABLE)
)

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE DETAIL {PARTITIONED_TABLE}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Analyze shuffle operations

# COMMAND ----------

fact = spark.table(FACT_TABLE)

normal_agg = (
    fact.groupBy("product_sk")
    .agg(
        F.sum("price").alias("total_sales"),
        F.count("*").alias("total_items")
    )
)

normal_agg.explain("formatted")

# COMMAND ----------

repartitioned_fact = fact.repartition(
    16,
    "product_sk"
)

repartitioned_agg = (
    repartitioned_fact.groupBy("product_sk")
    .agg(
        F.sum("price").alias("total_sales"),
        F.count("*").alias("total_items")
    )
)

repartitioned_agg.explain("formatted")

# COMMAND ----------

start = time.perf_counter()

normal_agg.count()

normal_agg_time = time.perf_counter() - start

print("Normal aggregation:", normal_agg_time)

# COMMAND ----------

start = time.perf_counter()

repartitioned_agg.count()

repartitioned_agg_time = time.perf_counter() - start

print("Repartitioned aggregation:", repartitioned_agg_time)

# COMMAND ----------

fact = spark.table(FACT_TABLE)
products = spark.table(PRODUCT_TABLE)

analysis_df = (
    fact.join(
        products,
        on="product_sk",
        how="inner"
    )
    .groupBy("product_sk")
    .agg(
        F.sum("price").alias("total_sales")
    )
    .orderBy(
        F.desc("total_sales")
    )
)

# COMMAND ----------

print("SIMPLE PLAN")
analysis_df.explain("simple")

print("EXTENDED PLAN")
analysis_df.explain("extended")

print("FORMATTED PLAN")
analysis_df.explain("formatted")

print("COST PLAN")
analysis_df.explain("cost")

# COMMAND ----------

# MAGIC %md
# MAGIC Delta OPTIMIZE and file compaction

# COMMAND ----------

OPTIMIZE_TABLE = (
    f"{CATALOG}.{SCHEMA}.fact_order_items_optimize_test"
)

spark.sql(f"""
CREATE OR REPLACE TABLE {OPTIMIZE_TABLE}
USING DELTA
AS
SELECT *
FROM {FACT_TABLE}
""")

# COMMAND ----------

before_detail = spark.sql(
    f"DESCRIBE DETAIL {OPTIMIZE_TABLE}"
).first()

print("Files before:", before_detail["numFiles"])
print("Size before:", before_detail["sizeInBytes"])

# COMMAND ----------

spark.sql(f"""
OPTIMIZE {OPTIMIZE_TABLE}
""")

# COMMAND ----------

after_detail = spark.sql(
    f"DESCRIBE DETAIL {OPTIMIZE_TABLE}"
).first()

print("Files after:", after_detail["numFiles"])
print("Size after:", after_detail["sizeInBytes"])

# COMMAND ----------

before_files = before_detail["numFiles"]
after_files = after_detail["numFiles"]

if before_files > 0:
    reduction = (
        (before_files - after_files)
        / before_files
    ) * 100

    print(
        "File count reduction:",
        round(reduction, 2),
        "%"
    )
else:
    print("No files to compare")

# COMMAND ----------

results = [
    ("Regular Join", "Record from Spark UI", "Record from Spark UI"),
    ("Broadcast Join", "Record from Spark UI", "Record from Spark UI"),
    ("Repartition", "Record write time", "Record file count"),
    ("Coalesce", "Record write time", "Record file count"),
    ("Normal Aggregation", "Record execution time", "Record shuffle metrics"),
    ("Repartitioned Aggregation", "Record execution time", "Record shuffle metrics"),
    ("OPTIMIZE", "Record files before", "Record files after")
]

results_df = spark.createDataFrame(
    results,
    ["Experiment", "Before / Baseline", "After / Comparison"]
)

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cleanup after all experiments

# COMMAND ----------

test_tables = [
    MERGE_TABLE,
    MERGE_SOURCE,
    SCHEMA_TABLE,
    REPARTITION_TABLE,
    COALESCE_TABLE,
    PARTITIONED_TABLE,
    OPTIMIZE_TABLE
]

for table_name in test_tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        print(f"Dropped: {table_name}")
    except Exception as e:
        print(f"Could not drop {table_name}: {e}")

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC