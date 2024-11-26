import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import logging

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Verify and Load FactSales with StoreKey") \
    .getOrCreate()

# Paths
gold_base_path = "output/gold/"
silver_base_path = "output/silver/"
date_str = datetime.now().strftime("%Y-%m-%d")

# Logging setup
log_dir = os.path.join("logs", "gold_data", date_str)
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "fact_sales_storekey_check.log")
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def log_message(message, level="info"):
    """Logs messages with the specified level."""
    if level == "info":
        logging.info(message)
    elif level == "error":
        logging.error(message)
    print(message)

def save_gold_data(df, table_name):
    """
    Saves transformed data into the Gold directory.
    Args:
        df: Spark DataFrame to save.
        table_name: Name of the table (e.g., fact_sales).
    """
    path = os.path.join(gold_base_path, table_name, date_str)
    try:
        df.write.mode("overwrite").parquet(path)
        log_message(f"Saved {table_name} to {path}")
    except Exception as e:
        log_message(f"Error saving {table_name}: {e}", level="error")
        raise

# Step 1: Load Dimensions and Sales Data
log_message("Loading dimension tables and sales data...")
df_store_gold = spark.read.parquet(os.path.join(gold_base_path, "dim_store", date_str))
df_products_gold = spark.read.parquet(os.path.join(gold_base_path, "dim_products", date_str))
df_sales = spark.read.parquet(os.path.join(silver_base_path, "enrichment/sales/with_currency", date_str))

# Step 2: Join Products with Stores
log_message("Joining products with stores...")
df_products_with_store = df_products_gold.join(
    df_store_gold,
    df_products_gold["SupplierID"] == df_store_gold["StoreID"],
    "left"
).select("ProductID", "StoreKey")

# Step 3: Join Sales with Products-Stores
log_message("Joining sales with products-stores...")
df_sales_with_store = df_sales.join(
    df_products_with_store,
    "ProductID",
    "left"
)

# Step 4: Verify StoreKey Validity
log_message("Verifying StoreKey validity...")
df_invalid_storekey = df_sales_with_store.join(
    df_store_gold,
    "StoreKey",
    "left_anti"  # Find rows where StoreKey in sales doesn't exist in DimStore
)

invalid_count = df_invalid_storekey.count()
log_message(f"Number of invalid StoreKey values: {invalid_count}", level="error")

if invalid_count > 0:
    log_message("Invalid StoreKey details:")
    df_invalid_storekey.select("StoreKey").distinct().show(truncate=False)

# Step 5: Filter Valid Rows and Save FactSales
log_message("Filtering valid rows for FactSales...")
df_fact_sales = df_sales_with_store.join(
    df_store_gold,
    "StoreKey",
    "inner"  # Only keep rows with valid StoreKey
)

log_message(f"Final FactSales contains {df_fact_sales.count()} valid records.")
save_gold_data(df_fact_sales, "fact_sales")

# Stop Spark session
spark.stop()
log_message("Spark session stopped.")
