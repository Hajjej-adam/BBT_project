from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import os
import logging

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Anomalies Detection in Sales and Products") \
    .getOrCreate()

# Paths
silver_base_path = "output/silver/"
date_str = datetime.now().strftime("%Y-%m-%d")
sales_path = os.path.join(silver_base_path, "enrichment", "sales", "with_currency",date_str)
products_path = os.path.join(silver_base_path, "enrichment", "products", date_str)
anomalies_sales_path = os.path.join(silver_base_path, "anomalies", "sales", date_str)
anomalies_products_path = os.path.join(silver_base_path, "anomalies", "products", date_str)
anomalies_summary_path = os.path.join("logs", "anomalies_summary", f"{date_str}.txt")

# Logging setup
log_dir = os.path.join("logs", "anomalies_detection", date_str)
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, "anomalies_detection.log")
os.makedirs(os.path.dirname(anomalies_summary_path), exist_ok=True)

logging.basicConfig(filename=log_path, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def log_message(message, level="info"):
    """Logs messages with the specified level."""
    if level == "info":
        logging.info(message)
    elif level == "error":
        logging.error(message)
    print(message)

# Load datasets
df_sales = spark.read.parquet(sales_path)
df_products = spark.read.parquet(products_path)

# --- SALES DATA ANOMALIES ---
# Future Dates
df_sales = df_sales.withColumn(
    "is_future_date",
    F.when(F.col("OrderDate") > F.lit(datetime.now()), True).otherwise(False)
)

# Unrealistic Quantity
df_sales = df_sales.withColumn(
    "is_unrealistic_quantity",
    F.when(F.col("Quantity") > 10000, True).otherwise(False)
)

# Low or Negative Total Amount
df_sales = df_sales.withColumn(
    "is_low_or_negative_amount",
    F.when(F.col("total_amount") <= 0, True).otherwise(False)
)

# Combine anomalies into a single column
df_sales = df_sales.withColumn(
    "anomaly_type",
    F.when(F.col("is_future_date"), "Future Date")
     .when(F.col("is_unrealistic_quantity"), "Unrealistic Quantity")
     .when(F.col("is_low_or_negative_amount"), "Low or Negative Amount")
     .otherwise(None)
)

# Count anomalies
sales_anomalies_count = df_sales.filter(F.col("anomaly_type").isNotNull()).count()
log_message(f"Detected {sales_anomalies_count} anomalies in sales data.")

# Save sales anomalies to parquet
columns_to_drop = ["is_low_or_negative_amount", "is_unrealistic_quantity", "is_future_date"]
df_sales = df_sales.drop(*columns_to_drop)

df_sales.write.mode("overwrite").parquet(anomalies_sales_path)
log_message(f"Sales anomalies saved to {anomalies_sales_path}")

# --- PRODUCTS DATA ANOMALIES ---
# Get the last year's date
from datetime import timedelta
one_year_ago = datetime.now() - timedelta(days=365)

# Debugging: Ensure the schema of df_sales is correct
log_message("Schema of df_sales before filtering:")
df_sales.printSchema()

# Filter sales data for the last year
df_product_sales = df_sales.filter(F.col("OrderDate") >= F.lit(one_year_ago)) \
    .select("ProductID").distinct()

# Debugging: Show the filtered sales data
log_message("Filtered product sales data (last year):")
df_product_sales.show()

# Join products with sales to identify products without sales in the last year
# Resolve ambiguity by renaming `ProductID` from df_product_sales
df_products = df_products.join(
    df_product_sales.withColumnRenamed("ProductID", "SalesProductID"),
    df_products["ProductID"] == F.col("SalesProductID"),
    how="left"
)

# Debugging: Check the schema after the join
log_message("Schema after joining products and product sales:")
df_products.printSchema()

# Add anomaly column for products marked active but with no sales
df_products = df_products.withColumn(
    "is_active_no_sales",
    F.when((F.col("product_status") == "Active") & F.col("SalesProductID").isNull(), True)
     .otherwise(False)
)

# Debugging: Check for anomalies
log_message("Products with anomalies (active but no sales):")
df_products.filter(F.col("is_active_no_sales")).show()

# Combine anomalies into a single column
df_products = df_products.withColumn(
    "anomaly_type",
    F.when(F.col("is_active_no_sales"), "Active No Sales")
     .otherwise(None)
)

# Drop the renamed column to keep the DataFrame clean
df_products = df_products.drop("SalesProductID")

# Debugging: Final anomalies
log_message("Final products data with anomalies:")
df_products.show()


# Count anomalies
products_anomalies_count = df_products.filter(F.col("anomaly_type").isNotNull()).count()
log_message(f"Detected {products_anomalies_count} anomalies in products data.")

# Save products anomalies to parquet
columns_to_drop_p = ["is_active_no_sales"]
df_products = df_products.drop(*columns_to_drop_p)
df_products.write.mode("overwrite").parquet(anomalies_products_path)
log_message(f"Products anomalies saved to {anomalies_products_path}")

# --- WRITE SUMMARY TO TEXT FILE ---
with open(anomalies_summary_path, "w") as f:
    f.write("Anomalies Detection Summary\n")
    f.write("===========================\n")
    f.write(f"Date: {date_str}\n\n")
    f.write(f"Sales Anomalies: {sales_anomalies_count}\n")
    f.write(f"Products Anomalies: {products_anomalies_count}\n\n")
    f.write("Details:\n")
    f.write(f"- Sales anomalies saved to: {anomalies_sales_path}\n")
    f.write(f"- Products anomalies saved to: {anomalies_products_path}\n")

log_message(f"Anomalies summary written to {anomalies_summary_path}")

# Stop Spark session
spark.stop()
