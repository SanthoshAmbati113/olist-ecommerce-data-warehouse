# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import types as T
spark.sql("USE CATALOG `e-commerce`")
spark.sql("USE SCHEMA ecommerce_silver")
BRONZE_SCHEMA = "ecommerce_bronze"
SILVER_SCHEMA = "ecommerce_silver"

print("Silver pipeline initialized!")

# COMMAND ----------

# MAGIC %md
# MAGIC Define metadata configuration
# MAGIC

# COMMAND ----------

table_config = {

    "customers": {
        "primary_keys": ["customer_id"],
        "casts": {}
    },

    "orders": {
        "primary_keys": ["order_id"],
        "casts": {
            "order_purchase_timestamp": "timestamp",
            "order_approved_at": "timestamp",
            "order_delivered_carrier_date": "timestamp",
            "order_delivered_customer_date": "timestamp",
            "order_estimated_delivery_date": "timestamp"
        }
    },

    "order_items": {
        "primary_keys": ["order_id", "order_item_id"],
        "casts": {
            "order_item_id": "int",
            "price": "double",
            "freight_value": "double",
            "shipping_limit_date": "timestamp"
        }
    },

    "payments": {
        "primary_keys": ["order_id", "payment_sequential"],
        "casts": {
            "payment_sequential": "int",
            "payment_installments": "int",
            "payment_value": "double"
        }
    },

    "reviews": {
        "primary_keys": ["review_id"],
        "casts": {
            "review_score": "int",
            "review_creation_date": "timestamp",
            "review_answer_timestamp": "timestamp"
        }
    },

    "products": {
        "primary_keys": ["product_id"],
        "casts": {
            "product_name_lenght": "int",
            "product_description_lenght": "int",
            "product_photos_qty": "int",
            "product_weight_g": "double",
            "product_length_cm": "double",
            "product_height_cm": "double",
            "product_width_cm": "double"
        }
    },

    "sellers": {
        "primary_keys": ["seller_id"],
        "casts": {}
    },

    "geolocation": {
        "primary_keys": [
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng"
        ],
        "casts": {
            "geolocation_lat": "double",
            "geolocation_lng": "double"
        }
    },

    "category_translation": {
        "primary_keys": ["product_category_name"],
        "casts": {}
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC Transform

# COMMAND ----------

def transform_to_silver(table_name, config):

    print(f"\nProcessing: {table_name}")

    # 1. Read Bronze table
    bronze_table = f"bronze_{table_name}"

    df = spark.table(
        f"`e-commerce`.{BRONZE_SCHEMA}.{bronze_table}"
    )

    initial_count = df.count()

    # 2. Trim whitespace from string columns
    for column_name, data_type in df.dtypes:

        if data_type == "string":
            df = df.withColumn(
                column_name,
                F.trim(F.col(column_name))
            )

    # 3. Remove records with null primary keys
    primary_keys = config["primary_keys"]

    for key in primary_keys:
        df = df.filter(F.col(key).isNotNull())

    # 4. Remove duplicate records
    df = df.dropDuplicates(primary_keys)

    # 5. Apply data type conversions
    for column_name, data_type in config["casts"].items():

        df = df.withColumn(
            column_name,
            F.col(column_name).cast(data_type)
        )

    # 6. Add processing metadata
    df = (
        df
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_source_layer", F.lit("bronze"))
    )

    # 7. Write Silver Delta table
    silver_table = f"silver_{table_name}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_table)
    )

    # 8. Calculate metrics
    final_count = df.count()

    print(f"Completed: {silver_table}")
    print(f"Initial records: {initial_count}")
    print(f"Final records: {final_count}")
    print(f"Removed records: {initial_count - final_count}")

    return {
        "table_name": silver_table,
        "initial_count": initial_count,
        "final_count": final_count,
        "removed_count": initial_count - final_count
    }

# COMMAND ----------

pipeline_results = []

for table_name, config in table_config.items():

    result = transform_to_silver(
        table_name,
        config
    )

    pipeline_results.append(result)

print("\nALL SILVER TABLES PROCESSED!")

# COMMAND ----------

VALID_ORDER_STATUSES = [
    "created",
    "approved",
    "invoiced",
    "processing",
    "shipped",
    "delivered",
    "unavailable",
    "canceled"
]

VALID_PAYMENT_TYPES = [
    "credit_card",
    "boleto",
    "voucher",
    "debit_card",
    "not_defined"
]

# COMMAND ----------

def add_validation(df, condition, reason):

    return df.withColumn(
        "_dq_reason",
        F.when(
            condition,
            F.concat_ws(
                "; ",
                F.col("_dq_reason"),
                F.lit(reason)
            )
        ).otherwise(F.col("_dq_reason"))
    )

# COMMAND ----------

def initialize_quality_check(df):

    return df.withColumn(
        "_dq_reason",
        F.lit(None).cast("string")
    )

# COMMAND ----------

def split_valid_invalid(df):

    valid_df = df.filter(
        F.col("_dq_reason").isNull()
    ).drop("_dq_reason")

    invalid_df = df.filter(
        F.col("_dq_reason").isNotNull()
    )

    return valid_df, invalid_df

# COMMAND ----------

def validate_foreign_key(
    df,
    child_column,
    parent_table,
    parent_column,
    reason
):

    parent_df = (
        spark.table(
            f"`e-commerce`.{SILVER_SCHEMA}.{parent_table}"
        )
        .select(parent_column)
        .distinct()
    )

    missing_parent = (
        df.alias("child")
        .join(
            parent_df.alias("parent"),
            F.col(f"child.{child_column}") ==
            F.col(f"parent.{parent_column}"),
            "left_anti"
        )
        .select(F.col("child.*"))
    )

    valid_parent = (
        df.alias("child")
        .join(
            parent_df.alias("parent"),
            F.col(f"child.{child_column}") ==
            F.col(f"parent.{parent_column}"),
            "left_semi"
        )
    )

    return missing_parent, valid_parent

# COMMAND ----------

# MAGIC %md
# MAGIC Business Validation Engine

# COMMAND ----------

def apply_business_rules(df, table_name):

    df = initialize_quality_check(df)

    # -------------------------
    # ORDERS
    # -------------------------

    if table_name == "orders":

        df = add_validation(
            df,
            ~F.col("order_status").isin(VALID_ORDER_STATUSES)
            | F.col("order_status").isNull(),
            "Invalid order status"
        )

        df = add_validation(
            df,
            F.col("order_delivered_customer_date").isNotNull()
            &
            F.col("order_purchase_timestamp").isNotNull()
            &
            (
                F.col("order_delivered_customer_date")
                < F.col("order_purchase_timestamp")
            ),
            "Delivery before purchase"
        )

        df = add_validation(
            df,
            F.col("order_approved_at").isNotNull()
            &
            F.col("order_purchase_timestamp").isNotNull()
            &
            (
                F.col("order_approved_at")
                < F.col("order_purchase_timestamp")
            ),
            "Approval before purchase"
        )

    # -------------------------
    # ORDER ITEMS
    # -------------------------

    elif table_name == "order_items":

        df = add_validation(
            df,
            F.col("price").isNull() | (F.col("price") < 0),
            "Invalid item price"
        )

        df = add_validation(
            df,
            F.col("freight_value").isNull()
            | (F.col("freight_value") < 0),
            "Invalid freight value"
        )

        df = add_validation(
            df,
            F.col("order_item_id").isNull()
            | (F.col("order_item_id") < 1),
            "Invalid order item ID"
        )

    # -------------------------
    # PAYMENTS
    # -------------------------

    elif table_name == "payments":

        df = add_validation(
            df,
            F.col("payment_value").isNull()
            | (F.col("payment_value") < 0),
            "Invalid payment value"
        )

        df = add_validation(
            df,
            ~F.col("payment_type").isin(VALID_PAYMENT_TYPES)
            | F.col("payment_type").isNull(),
            "Invalid payment type"
        )

        df = add_validation(
            df,
            F.col("payment_installments").isNull()
            | (F.col("payment_installments") < 1),
            "Invalid payment installments"
        )

    # -------------------------
    # REVIEWS
    # -------------------------

    elif table_name == "reviews":

        df = add_validation(
            df,
            F.col("review_score").isNull()
            | (~F.col("review_score").between(1, 5)),
            "Review score must be between 1 and 5"
        )

    # -------------------------
    # PRODUCTS
    # -------------------------

    elif table_name == "products":

        numeric_columns = [
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm"
        ]

        for column_name in numeric_columns:

            df = add_validation(
                df,
                F.col(column_name).isNotNull()
                & (F.col(column_name) < 0),
                f"Negative value in {column_name}"
            )

    # -------------------------
    # GEOLOCATION
    # -------------------------

    elif table_name == "geolocation":

        df = add_validation(
            df,
            F.col("geolocation_lat").isNull()
            | ~F.col("geolocation_lat").between(-90, 90),
            "Invalid latitude"
        )

        df = add_validation(
            df,
            F.col("geolocation_lng").isNull()
            | ~F.col("geolocation_lng").between(-180, 180),
            "Invalid longitude"
        )

    return df

# COMMAND ----------

def add_fk_validation(
    df,
    child_column,
    parent_table,
    parent_column,
    reason
):

    parent_df = (
        spark.table(
            f"`e-commerce`.{SILVER_SCHEMA}.{parent_table}"
        )
        .select(parent_column)
        .distinct()
        .withColumn("_parent_exists", F.lit(1))
    )

    joined_df = df.join(
        parent_df,
        df[child_column] == parent_df[parent_column],
        "left"
    )

    joined_df = joined_df.withColumn(
        "_dq_reason",
        F.when(
            F.col("_parent_exists").isNull(),
            F.concat_ws(
                "; ",
                F.col("_dq_reason"),
                F.lit(reason)
            )
        ).otherwise(F.col("_dq_reason"))
    )

    return joined_df.drop(
        parent_column,
        "_parent_exists"
    )

# COMMAND ----------

def process_silver_table(table_name, foreign_keys=None):

    print(f"\nStarting quality checks: {table_name}")

    # Read existing Phase 1 Silver table
    df = spark.table(
        f"`e-commerce`.{SILVER_SCHEMA}.silver_{table_name}"
    )

    initial_count = df.count()

    # Apply business rules
    df = apply_business_rules(df, table_name)

    # Apply foreign-key validations
    if foreign_keys:

        for fk in foreign_keys:

            df = add_fk_validation(
                df,
                fk["child_column"],
                fk["parent_table"],
                fk["parent_column"],
                fk["reason"]
            )

    # Special handling for category enrichment
    if table_name == "products":

        translations = (
            spark.table(
                f"`e-commerce`.{SILVER_SCHEMA}."
                "silver_category_translation"
            )
            .select(
                "product_category_name",
                F.col("product_category_name_english")
                .alias("_translated_category")
            )
            .dropDuplicates(["product_category_name"])
        )
        df = df.drop("product_category_name_english")

        df = df.join(
            translations,
            on="product_category_name",
            how="left"
        )

        df = df.withColumnRenamed(
            "_translated_category",
            "product_category_name_english"
        )

        df = add_validation(
            df,
            F.col("product_category_name").isNotNull()
            & F.col("product_category_name_english").isNull(),
            "Product category translation missing"
        )

    # Split records
    valid_df, invalid_df = split_valid_invalid(df)

    # Save valid records
    (
        valid_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"silver_{table_name}")
    )

    # Save invalid records separately
    quarantine_table = f"dq_quarantine_{table_name}"

    (
        invalid_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(quarantine_table)
    )

    # Execution metrics
    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    print(f"Table: silver_{table_name}")
    print(f"Input records: {initial_count}")
    print(f"Valid records: {valid_count}")
    print(f"Quarantined records: {invalid_count}")

    return {
        "table_name": table_name,
        "input_records": initial_count,
        "valid_records": valid_count,
        "invalid_records": invalid_count
    }

# COMMAND ----------

foreign_key_config = {

    "orders": [
        {
            "child_column": "customer_id",
            "parent_table": "silver_customers",
            "parent_column": "customer_id",
            "reason": "Customer does not exist"
        }
    ],

    "order_items": [
        {
            "child_column": "order_id",
            "parent_table": "silver_orders",
            "parent_column": "order_id",
            "reason": "Order does not exist"
        },
        {
            "child_column": "product_id",
            "parent_table": "silver_products",
            "parent_column": "product_id",
            "reason": "Product does not exist"
        },
        {
            "child_column": "seller_id",
            "parent_table": "silver_sellers",
            "parent_column": "seller_id",
            "reason": "Seller does not exist"
        }
    ],

    "payments": [
        {
            "child_column": "order_id",
            "parent_table": "silver_orders",
            "parent_column": "order_id",
            "reason": "Order does not exist"
        }
    ],

    "reviews": [
        {
            "child_column": "order_id",
            "parent_table": "silver_orders",
            "parent_column": "order_id",
            "reason": "Order does not exist"
        }
    ]
}

# COMMAND ----------

def add_fk_validation(
    df,
    child_column,
    parent_table,
    parent_column,
    reason
):

    parent_df = (
        spark.table(
            f"`e-commerce`.{SILVER_SCHEMA}.{parent_table}"
        )
        .select(
            F.col(parent_column).alias("_parent_key")
        )
        .distinct()
        .withColumn("_parent_exists", F.lit(1))
    )

    child = df.alias("child")

    joined = child.join(
        parent_df.alias("parent"),
        F.col(f"child.{child_column}") ==
        F.col("parent._parent_key"),
        "left"
    )

    result = joined.select(
        "child.*",
        F.col("parent._parent_exists")
    )

    result = result.withColumn(
        "_dq_reason",
        F.when(
            F.col("_parent_exists").isNull(),
            F.concat_ws(
                "; ",
                F.col("_dq_reason"),
                F.lit(reason)
            )
        ).otherwise(F.col("_dq_reason"))
    )

    return result.drop("_parent_exists")

# COMMAND ----------

execution_order = [
    "customers",
    "sellers",
    "category_translation",
    "products",
    "geolocation",
    "orders",
    "order_items",
    "payments",
    "reviews"
]

# COMMAND ----------

quality_results = []

for table_name in execution_order:

    fk_rules = foreign_key_config.get(
        table_name,
        []
    )

    result = process_silver_table(
        table_name,
        fk_rules
    )

    quality_results.append(result)

print("ADVANCED SILVER PIPELINE COMPLETED!")

# COMMAND ----------

# MAGIC %md
# MAGIC Execution Metrics

# COMMAND ----------

quality_report = spark.createDataFrame(
    quality_results
)

quality_report = quality_report.withColumn(
    "quality_percentage",
    F.round(
        F.col("valid_records") /
        F.when(
            F.col("input_records") == 0,
            None
        ).otherwise(F.col("input_records")) * 100,
        2
    )
)

display(quality_report)

# COMMAND ----------

display(
    spark.table(
        "dq_quarantine_payments"
    )
)

# COMMAND ----------

display(
    spark.table(
        "dq_quarantine_payments"
    )
    .groupBy("_dq_reason")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# List Silver tables

display(
    spark.sql("""
        SHOW TABLES IN `e-commerce`.ecommerce_silver
    """)
)