from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pycountry
import os
from datetime import datetime
import logging

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Enrichment - Region Code and Client Status") \
    .getOrCreate()

# Define paths
silver_base_path = "output/silver/"
date_str = datetime.now().strftime("%Y-%m-%d")

# Log path organized by date
log_dir = os.path.join("logs", "data_enrichment", date_str)
log_path = os.path.join(log_dir, "data_enrichment.log")

# Ensure the logs directory exists
os.makedirs(log_dir, exist_ok=True)

# Configure logging to write to the log file
logging.basicConfig(filename=log_path, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def log_message(message, level="info"):
    """Logs messages with the specified level."""
    if level == "info":
        logging.info(message)
    elif level == "error":
        logging.error(message)
    print(message)

# UDF to convert country names to region codes
def get_region_code(country_name):
    """
    Converts a country name to a region code (e.g., 'NA' for North America).
    
    Parameters:
        country_name (str): Name of the country to convert.
    
    Returns:
        str: Continent code or "Unknown" if not found.
    """
    try:
        # Handle country name variations
        if country_name.lower() in ["uk", "united kingdom"]:
            country_name = "United Kingdom"
        # Get the ISO-3166 alpha-3 country code
        country_code = pycountry.countries.lookup(country_name).alpha_3
        return country_code
    except (LookupError, KeyError):
        return "Unknown"

# Register the UDF for PySpark
get_region_code_udf = F.udf(get_region_code)

# Load cleaned customers data
customers_path = os.path.join(silver_base_path, "cleaned", "customers", date_str)
df_customers_cleaned = spark.read.parquet(customers_path)

# Load cleaned sales data
sales_path = os.path.join(silver_base_path, "cleaned", "sales", date_str)
df_sales_cleaned = spark.read.parquet(sales_path)

# Enrich with region code based on the country
df_customers_enriched = df_customers_cleaned.withColumn("code_region", get_region_code_udf(F.col("Country")))

# Add a TotalAmount column to the sales data
df_sales_cleaned = df_sales_cleaned.withColumn(
    "TotalAmount",
    F.col("UnitPrice") * F.col("Quantity") * (1 - F.col("Discount"))
)

# Calculate total purchase amount for each client
df_total_purchase = df_sales_cleaned.groupBy("CustomerID").agg(
    F.sum("TotalAmount").alias("total_purchase_amount")
)


# Join customers data with total purchase amount
df_customers_with_purchase = df_customers_enriched.join(
    df_total_purchase,
    on="CustomerID",
    how="left"
).fillna({"total_purchase_amount": 0})  # Fill missing values with 0

# Define client status based on total purchase amount
df_customers_final = df_customers_with_purchase.withColumn(
    "status_client",
    F.when(F.col("total_purchase_amount") > 10000, "VIP")
     .when((F.col("total_purchase_amount") >= 1000) & (F.col("total_purchase_amount") <= 10000), "Regular")
     .otherwise("Inactive")
)


# Select only the desired columns (existing columns + added columns)
columns_to_save = df_customers_cleaned.columns + ["code_region", "status_client"]

df_customers_final = df_customers_final.select(*columns_to_save)

# Configure logging to write to the log file
logging.basicConfig(filename=log_path, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def log_message(message, level="info"):
    """Logs messages with the specified level."""
    if level == "info":
        logging.info(message)
    elif level == "error":
        logging.error(message)
    print(message)

# Load cleaned products data
products_path = os.path.join(silver_base_path, "cleaned", "products", date_str)
df_products_cleaned = spark.read.parquet(products_path)

# Define product status based on adjusted logic
df_products_with_status = df_products_cleaned.withColumn(
    "product_status",
    F.when(F.col("Discontinued") == 1, "Discontinued")  # Discontinued products
     .when(F.col("UnitsInStock") < 10, "Low Stock")  # Low stock threshold
     .when((F.col("UnitsInStock") > 0) | (F.col("UnitsOnOrder") > 0), "Active")  # Active if stock or orders exist
     .otherwise("Inactive")  # Default to inactive
)

# Save enriched products data with product status
enriched_products_path = os.path.join(silver_base_path, "enrichment", "products", date_str)
df_products_with_status.write.mode("overwrite").parquet(enriched_products_path)
enriched_customers_path = os.path.join(silver_base_path, "enrichment", "customers", date_str)
df_customers_final.write.mode("overwrite").parquet(enriched_customers_path)

# Log message for completion
log_message(f"Enriched products data with 'product_status' and 'code_region' and 'status_client' saved to '{enriched_products_path}'")

# Stop Spark session
spark.stop()