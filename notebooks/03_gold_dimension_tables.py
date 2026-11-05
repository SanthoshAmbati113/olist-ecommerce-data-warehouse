# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import Window

# Select catalog
spark.sql("USE CATALOG `e-commerce`")

# Create Gold schema
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS ecommerce_gold
""")

spark.sql("USE SCHEMA ecommerce_gold")

BRONZE_SCHEMA = "ecommerce_bronze"
SILVER_SCHEMA = "ecommerce_silver"
GOLD_SCHEMA = "ecommerce_gold"

print("Gold layer initialized successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC Creating surrogate keys in order to connect dimension and fact tables

# COMMAND ----------

def generate_surrogate_key(df, columns, key_name):

    key_expression = F.pmod(
        F.xxhash64(
            *[F.coalesce(F.col(c).cast("string"), F.lit("NULL"))
              for c in columns]
        ),
        F.lit(9223372036854775806)
    ) + F.lit(1)

    return df.withColumn(
        key_name,
        key_expression.cast("long")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Build dim_geography

# COMMAND ----------

# Read Silver tables

customers = spark.table(
    f"`e-commerce`.{SILVER_SCHEMA}.silver_customers"
)

sellers = spark.table(
    f"`e-commerce`.{SILVER_SCHEMA}.silver_sellers"
)

geolocation = spark.table(
    f"`e-commerce`.{SILVER_SCHEMA}.silver_geolocation"
)

# COMMAND ----------

# Customer geography

customer_geo = customers.select(
    F.col("customer_zip_code_prefix")
        .cast("string").alias("zip_code_prefix"),
    F.initcap(F.trim("customer_city")).alias("city"),
    F.upper(F.trim("customer_state")).alias("state")
)

# Seller geography

seller_geo = sellers.select(
    F.col("seller_zip_code_prefix")
        .cast("string").alias("zip_code_prefix"),
    F.initcap(F.trim("seller_city")).alias("city"),
    F.upper(F.trim("seller_state")).alias("state")
)

# Combine customer and seller locations

all_geo = (
    customer_geo
    .unionByName(seller_geo)
    .dropDuplicates()
    .filter(
        F.col("zip_code_prefix").isNotNull()
        & F.col("city").isNotNull()
        & F.col("state").isNotNull()
    )
)

# COMMAND ----------

geo_coordinates = (
    geolocation
    .groupBy(
        F.col("geolocation_zip_code_prefix")
            .cast("string").alias("zip_code_prefix")
    )
    .agg(
        F.avg("geolocation_lat").alias("latitude"),
        F.avg("geolocation_lng").alias("longitude")
    )
)

# COMMAND ----------

dim_geography = all_geo.join(
    geo_coordinates,
    on="zip_code_prefix",
    how="left"
)

# COMMAND ----------

dim_geography = generate_surrogate_key(
    dim_geography,
    ["zip_code_prefix", "city", "state"],
    "geography_sk"
)

# COMMAND ----------

unknown_geo = spark.range(1).select(
    F.lit(-1).cast("long").alias("geography_sk"),
    F.lit(None).cast("string").alias("zip_code_prefix"),
    F.lit(None).cast("string").alias("city"),
    F.lit(None).cast("string").alias("state"),
    F.lit(None).cast("double").alias("latitude"),
    F.lit(None).cast("double").alias("longitude")
)

dim_geography = dim_geography.unionByName(
    unknown_geo
)

# COMMAND ----------

(
    dim_geography.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dim_geography")
)

print("dim_geography created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC Create dim_customers

# COMMAND ----------

customers = spark.table(
    f"`e-commerce`.{SILVER_SCHEMA}.silver_customers"
)

# Standardize customer attributes

dim_customers = customers.select(
    "customer_id",
    "customer_unique_id",
    F.col("customer_zip_code_prefix")
        .cast("string").alias("zip_code_prefix"),
    F.initcap(F.trim("customer_city")).alias("city"),
    F.upper(F.trim("customer_state")).alias("state")
).dropDuplicates(["customer_id"])

# COMMAND ----------

dim_customers = generate_surrogate_key(
    dim_customers,
    ["customer_id"],
    "customer_sk"
)

# COMMAND ----------

geo_lookup = dim_geography.select(
    "zip_code_prefix",
    "city",
    "state",
    "geography_sk"
)

dim_customers = dim_customers.join(
    geo_lookup,
    on=["zip_code_prefix", "city", "state"],
    how="left"
)

dim_customers = dim_customers.withColumn(
    "geography_sk",
    F.coalesce(
        F.col("geography_sk"),
        F.lit(-1)
    )
)

# COMMAND ----------

(
    dim_customers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dim_customers")
)

print("dim_customers created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC Create dim_sellers

# COMMAND ----------

sellers = spark.table(
    f"`e-commerce`.{SILVER_SCHEMA}.silver_sellers"
)

dim_sellers = sellers.select(
    "seller_id",
    F.col("seller_zip_code_prefix")
        .cast("string").alias("zip_code_prefix"),
    F.initcap(F.trim("seller_city")).alias("city"),
    F.upper(F.trim("seller_state")).alias("state")
).dropDuplicates(["seller_id"])

# COMMAND ----------

dim_sellers = generate_surrogate_key(
    dim_sellers,
    ["seller_id"],
    "seller_sk"
)

# COMMAND ----------

dim_sellers = dim_sellers.join(
    geo_lookup,
    on=["zip_code_prefix", "city", "state"],
    how="left"
)

dim_sellers = dim_sellers.withColumn(
    "geography_sk",
    F.coalesce(
        F.col("geography_sk"),
        F.lit(-1)
    )
)

# COMMAND ----------

(
    dim_sellers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dim_sellers")
)

print("dim_sellers created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC Create dim_products

# COMMAND ----------

products = spark.table(
    f"`e-commerce`.{SILVER_SCHEMA}.silver_products"
)

dim_products = products.select(
    "product_id",
    "product_category_name",
    "product_category_name_english",
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
).dropDuplicates(["product_id"])

# COMMAND ----------

dim_products = generate_surrogate_key(
    dim_products,
    ["product_id"],
    "product_sk"
)

# COMMAND ----------

dim_products = dim_products.withColumn(
    "product_category",
    F.coalesce(
        F.col("product_category_name_english"),
        F.col("product_category_name"),
        F.lit("Unknown")
    )
)

# COMMAND ----------

(
    dim_products.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dim_products")
)

print("dim_products created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC Create dim_date

# COMMAND ----------

orders = spark.table(
    f"`e-commerce`.{SILVER_SCHEMA}.silver_orders"
)

date_range = orders.select(
    F.min(
        F.to_date("order_purchase_timestamp")
    ).alias("start_date"),

    F.max(
        F.to_date("order_purchase_timestamp")
    ).alias("end_date")
).first()

start_date = date_range["start_date"]
end_date = date_range["end_date"]

print("Start date:", start_date)
print("End date:", end_date)

# COMMAND ----------

# Calculate number of days

total_days = (end_date - start_date).days + 1

# Generate calendar dates

dim_date = (
    spark.range(total_days)
    .withColumn(
        "full_date",
        F.date_add(
            F.lit(start_date),
            F.col("id").cast("int")
        )
    )
    .drop("id")
)

# COMMAND ----------

dim_date = (
    dim_date

    # Date surrogate key: YYYYMMDD
    .withColumn(
        "date_key",
        F.date_format("full_date", "yyyyMMdd").cast("int")
    )

    .withColumn("day", F.dayofmonth("full_date"))

    .withColumn("month", F.month("full_date"))

    .withColumn(
        "month_name",
        F.date_format("full_date", "MMMM")
    )

    .withColumn("quarter", F.quarter("full_date"))

    .withColumn("year", F.year("full_date"))

    .withColumn(
        "week_of_year",
        F.weekofyear("full_date")
    )

    .withColumn(
        "day_of_week",
        F.date_format("full_date", "EEEE")
    )

    .withColumn(
        "day_of_week_number",
        F.dayofweek("full_date")
    )

    .withColumn(
        "is_weekend",
        F.dayofweek("full_date").isin(1, 7)
    )

    .withColumn(
        "year_month",
        F.date_format("full_date", "yyyy-MM")
    )

    .withColumn(
        "year_quarter",
        F.concat(
            F.col("year"),
            F.lit("-Q"),
            F.col("quarter")
        )
    )
)

# COMMAND ----------

(
    dim_date.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dim_date")
)

print("dim_date created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC Validate all five dimension tables

# COMMAND ----------

display(
    spark.sql("""
        SHOW TABLES IN `e-commerce`.ecommerce_gold
    """)
)

# COMMAND ----------

dimension_tables = [
    "dim_customers",
    "dim_products",
    "dim_sellers",
    "dim_geography",
    "dim_date"
]

for table in dimension_tables:

    df = spark.table(table)

    print(
        f"{table}: {df.count()} records"
    )

# COMMAND ----------

for table in dimension_tables:

    print(f"\n========== {table} ==========")

    spark.table(table).printSchema()

# COMMAND ----------

for table in dimension_tables:

    df = spark.table(table)

    key = {
        "dim_customers": "customer_sk",
        "dim_products": "product_sk",
        "dim_sellers": "seller_sk",
        "dim_geography": "geography_sk",
        "dim_date": "date_key"
    }[table]

    total = df.count()
    distinct = df.select(key).distinct().count()

    print(
        table,
        "Total:", total,
        "Unique keys:", distinct,
        "Duplicates:", total - distinct
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Fact Tables

# COMMAND ----------

# MAGIC %md
# MAGIC Fact_Orders

# COMMAND ----------

orders = spark.table(
    f"`e-commerce`.{SILVER_SCHEMA}.silver_orders"
)

customers = spark.table(
    f"`e-commerce`.{GOLD_SCHEMA}.dim_customers"
)

dates = spark.table(
    f"`e-commerce`.{GOLD_SCHEMA}.dim_date"
)

# Customer lookup
customer_lookup = customers.select(
    "customer_id",
    "customer_sk",
    "geography_sk"
)

# Date lookups
purchase_dates = dates.select(
    F.col("full_date").alias("_purchase_date"),
    F.col("date_key").alias("purchase_date_key")
)

approval_dates = dates.select(
    F.col("full_date").alias("_approval_date"),
    F.col("date_key").alias("approval_date_key")
)

delivery_dates = dates.select(
    F.col("full_date").alias("_delivery_date"),
    F.col("date_key").alias("delivery_date_key")
)

estimated_dates = dates.select(
    F.col("full_date").alias("_estimated_date"),
    F.col("date_key").alias("estimated_delivery_date_key")
)

# Join orders with customer dimension
fact_orders = orders.join(
    customer_lookup,
    on="customer_id",
    how="left"
)

# Join purchase date
fact_orders = fact_orders.join(
    purchase_dates,
    F.to_date(fact_orders.order_purchase_timestamp)
    == purchase_dates._purchase_date,
    "left"
)

# Join approval date
fact_orders = fact_orders.join(
    approval_dates,
    F.to_date(fact_orders.order_approved_at)
    == approval_dates._approval_date,
    "left"
)

# Join actual delivery date
fact_orders = fact_orders.join(
    delivery_dates,
    F.to_date(fact_orders.order_delivered_customer_date)
    == delivery_dates._delivery_date,
    "left"
)

# Join estimated delivery date
fact_orders = fact_orders.join(
    estimated_dates,
    F.to_date(fact_orders.order_estimated_delivery_date)
    == estimated_dates._estimated_date,
    "left"
)

# COMMAND ----------

fact_orders = fact_orders.select(
    "order_id",
    "customer_sk",
    "geography_sk",

    "purchase_date_key",
    "approval_date_key",
    "delivery_date_key",
    "estimated_delivery_date_key",

    "order_status",

    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",

    F.datediff(
        F.to_date("order_delivered_customer_date"),
        F.to_date("order_purchase_timestamp")
    ).alias("delivery_days"),

    F.datediff(
        F.to_date("order_estimated_delivery_date"),
        F.to_date("order_purchase_timestamp")
    ).alias("estimated_delivery_days"),

    F.lit(1).alias("order_count")
)

# COMMAND ----------

(
    fact_orders.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("fact_orders")
)

print("fact_orders created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC Fact_Order_items

# COMMAND ----------

order_items = spark.table(
    f"`e-commerce`.{SILVER_SCHEMA}.silver_order_items"
)

orders = spark.table(
    f"`e-commerce`.{SILVER_SCHEMA}.silver_orders"
)

products = spark.table(
    f"`e-commerce`.{GOLD_SCHEMA}.dim_products"
)

sellers = spark.table(
    f"`e-commerce`.{GOLD_SCHEMA}.dim_sellers"
)

customers = spark.table(
    f"`e-commerce`.{GOLD_SCHEMA}.dim_customers"
)

# COMMAND ----------

product_lookup = products.select(
    "product_id",
    "product_sk"
)

seller_lookup = sellers.select(
    "seller_id",
    "seller_sk"
)

customer_lookup = customers.select(
    "customer_id",
    "customer_sk",
    "geography_sk"
)

# COMMAND ----------

order_lookup = orders.select(
    "order_id",
    "customer_id",
    "order_purchase_timestamp",
    "order_status"
)

fact_order_items = order_items.join(
    order_lookup,
    on="order_id",
    how="inner"
)

# COMMAND ----------

fact_order_items = (
    fact_order_items

    .join(
        product_lookup,
        on="product_id",
        how="left"
    )

    .join(
        seller_lookup,
        on="seller_id",
        how="left"
    )

    .join(
        customer_lookup,
        on="customer_id",
        how="left"
    )
)

# COMMAND ----------

date_lookup = dates.select(
    F.col("full_date").alias("_purchase_date"),
    F.col("date_key").alias("purchase_date_key")
)

fact_order_items = fact_order_items.join(
    date_lookup,
    F.to_date("order_purchase_timestamp")
    == date_lookup._purchase_date,
    "left"
)

# COMMAND ----------

fact_order_items = fact_order_items.select(

    "order_id",
    "order_item_id",

    "customer_sk",
    "product_sk",
    "seller_sk",
    "geography_sk",
    "purchase_date_key",

    "order_status",

    "price",
    "freight_value",

    (F.col("price") + F.col("freight_value"))
        .alias("item_total_value"),

    F.lit(1).alias("item_quantity"),

    "shipping_limit_date"
)

# COMMAND ----------

(
    fact_order_items.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("fact_order_items")
)

print("fact_order_items created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC Fact_payments

# COMMAND ----------

payments = spark.table(
    f"`e-commerce`.{SILVER_SCHEMA}.silver_payments"
)

orders = spark.table(
    f"`e-commerce`.{SILVER_SCHEMA}.silver_orders"
)

customers = spark.table(
    f"`e-commerce`.{GOLD_SCHEMA}.dim_customers"
)

# COMMAND ----------

order_lookup = orders.select(
    "order_id",
    "customer_id",
    "order_purchase_timestamp",
    "order_status"
)

customer_lookup = customers.select(
    "customer_id",
    "customer_sk",
    "geography_sk"
)

# COMMAND ----------

fact_payments = (
    payments

    .join(
        order_lookup,
        on="order_id",
        how="inner"
    )

    .join(
        customer_lookup,
        on="customer_id",
        how="left"
    )
)

# COMMAND ----------

fact_payments = fact_payments.join(
    date_lookup,
    F.to_date("order_purchase_timestamp")
    == date_lookup._purchase_date,
    "left"
)

# COMMAND ----------

fact_payments = fact_payments.select(

    "order_id",
    "payment_sequential",

    "customer_sk",
    "geography_sk",
    "purchase_date_key",

    "order_status",

    "payment_type",
    "payment_installments",
    "payment_value",

    F.lit(1).alias("payment_count")
)

# COMMAND ----------

(
    fact_payments.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("fact_payments")
)

print("fact_payments created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC Fact_reviews

# COMMAND ----------

reviews = spark.table(
    f"`e-commerce`.{SILVER_SCHEMA}.silver_reviews"
)

orders = spark.table(
    f"`e-commerce`.{SILVER_SCHEMA}.silver_orders"
)

# COMMAND ----------

order_lookup = orders.select(
    "order_id",
    "customer_id",
    "order_purchase_timestamp",
    "order_status"
)
customer_lookup = customers.select(
    "customer_id",
    "customer_sk",
    "geography_sk"
)

# COMMAND ----------

fact_reviews = (
    reviews

    .join(
        order_lookup,
        on="order_id",
        how="inner"
    )

    .join(
        customer_lookup,
        on="customer_id",
        how="left"
    )
)

# COMMAND ----------

fact_reviews = fact_reviews.join(
    date_lookup,
    F.to_date("order_purchase_timestamp")
    == date_lookup._purchase_date,
    "left"
)

# COMMAND ----------

review_date_lookup = dates.select(
    F.col("full_date").alias("_review_date"),
    F.col("date_key").alias("review_date_key")
)

fact_reviews = fact_reviews.join(
    review_date_lookup,
    F.to_date("review_creation_date")
    == review_date_lookup._review_date,
    "left"
)

# COMMAND ----------

fact_reviews = fact_reviews.select(

    "review_id",
    "order_id",

    "customer_sk",
    "geography_sk",

    "purchase_date_key",
    "review_date_key",

    "order_status",

    "review_score",
    "review_comment_title",
    "review_comment_message",

    "review_creation_date",
    "review_answer_timestamp",

    F.when(
        F.col("review_answer_timestamp").isNotNull(),
        1
    ).otherwise(0).alias("review_response_flag"),

    F.lit(1).alias("review_count")
)

# COMMAND ----------

(
    fact_reviews.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("fact_reviews")
)

print("fact_reviews created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC Validating

# COMMAND ----------

fact_tables = [
    "fact_orders",
    "fact_order_items",
    "fact_payments",
    "fact_reviews"
]

for table in fact_tables:

    df = spark.table(table)

    print(
        f"{table}: {df.count()} records"
    )

# COMMAND ----------

grain_config = {

    "fact_orders": ["order_id"],

    "fact_order_items": [
        "order_id",
        "order_item_id"
    ],

    "fact_payments": [
        "order_id",
        "payment_sequential"
    ],

    "fact_reviews": [
        "review_id",
        "order_id"
    ]
}

for table, grain_columns in grain_config.items():

    df = spark.table(table)

    total_records = df.count()

    unique_records = (
        df.select(*grain_columns)
        .distinct()
        .count()
    )

    print(f"\n{table}")
    print("Total records:", total_records)
    print("Unique grain combinations:", unique_records)
    print("Duplicate records:", total_records - unique_records)

# COMMAND ----------

# MAGIC %md
# MAGIC Validate surrogate key references

# COMMAND ----------

fact_dimension_checks = {

    "fact_orders": {
        "customer_sk": "dim_customers",
        "geography_sk": "dim_geography",
        "purchase_date_key": "dim_date"
    },

    "fact_order_items": {
        "customer_sk": "dim_customers",
        "product_sk": "dim_products",
        "seller_sk": "dim_sellers",
        "geography_sk": "dim_geography",
        "purchase_date_key": "dim_date"
    },

    "fact_payments": {
        "customer_sk": "dim_customers",
        "geography_sk": "dim_geography",
        "purchase_date_key": "dim_date"
    },

    "fact_reviews": {
        "customer_sk": "dim_customers",
        "geography_sk": "dim_geography",
        "purchase_date_key": "dim_date",
        "review_date_key": "dim_date"
    }
}

for fact, relationships in fact_dimension_checks.items():

    fact_df = spark.table(fact)

    print(f"\nChecking {fact}")

    for key_column, dimension in relationships.items():

        dimension_df = spark.table(dimension)

        missing = (
            fact_df
            .filter(F.col(key_column).isNotNull())
            .join(
                dimension_df.select(
                    F.col({
                        "dim_customers": "customer_sk",
                        "dim_products": "product_sk",
                        "dim_sellers": "seller_sk",
                        "dim_geography": "geography_sk",
                        "dim_date": "date_key"
                    }[dimension]).alias("_dimension_key")
                ),
                fact_df[key_column] ==
                F.col("_dimension_key"),
                "left_anti"
            )
            .count()
        )

        print(
            f"{key_column} -> {dimension}: "
            f"{missing} unmatched references"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC Fact table summary

# COMMAND ----------

fact_summary = []

for table in fact_tables:

    df = spark.table(table)

    fact_summary.append({
        "fact_table": table,
        "record_count": df.count(),
        "column_count": len(df.columns)
    })

display(
    spark.createDataFrame(fact_summary)
)

# COMMAND ----------

