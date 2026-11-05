# Project Setup and Execution

## Prerequisites

- Access to a Databricks workspace.
- Compute with PySpark and Delta Lake support.
- Unity Catalog permissions to create catalogs/schemas/tables or access the existing project objects.
- Olist Brazilian E-Commerce dataset CSV files.

## 1. Obtain the data

Download the Olist dataset from:

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Keep the CSV files outside this Git repository.

## 2. Create or verify Unity Catalog objects

The notebooks discussed for this project use:

- Catalog: `e-commerce`
- Schemas: `ecommerce_bronze`, `ecommerce_silver`, `ecommerce_gold`
- Volume: `raw_files`

The expected raw file directory is:

```text
/Volumes/e-commerce/ecommerce_bronze/raw_files/archive/
```

Upload the nine CSV files into this directory. If your actual Volume path differs, update the path in the Bronze ingestion notebook.

## 3. Import notebooks

Import the source files from `notebooks/` into your Databricks workspace. Review the notebook paths and catalog/schema variables before execution.

## 4. Execute in order

1. Bronze ingestion
2. Silver transformation and data-quality checks
3. Gold dimension creation
4. Gold fact creation
5. Delta Lake and Spark optimization experiments (after completing Phase 6)

Run each notebook successfully before moving to the next layer.

## 5. Dashboards

Create or import the Databricks SQL queries and dashboards in your workspace. Dashboard objects and permissions may need to be recreated in the target workspace; screenshots in this repository are documentation, not an importable dashboard bundle.

## Configuration and security

- Do not commit personal access tokens, passwords, OAuth secrets or `.databrickscfg`.
- Prefer Databricks secrets or approved workspace identity mechanisms for credentials.
- Replace hardcoded workspace-specific paths with configuration variables where practical.
- The project is not currently packaged as a one-command local deployment.
