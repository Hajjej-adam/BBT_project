import pycountry
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pycountry_convert as pc
import os
from datetime import datetime
import logging

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Enrichment - Continent Code and Client Status") \
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
    elif level == "warning":
        logging.warning(message)
    elif level == "error":
        logging.error(message)
    print(message)

# UDF to convert country names to continent codes
def get_continent_code(country_name):
    """
    Converts a country name to a continent code (e.g., 'NA' for North America).
    
    Parameters:
        country_name (str): Name of the country to convert.
    
    Returns:
        str: Continent code or "Unknown" if not found.
    """
    try:
        # Handle country name variations
        if country_name.lower() in ["uk", "united kingdom"]:
            country_name = "United Kingdom"
        country_alpha2 = pycountry.countries.lookup(country_name).alpha_2
        continent_code = pc.country_alpha2_to_continent_code(country_alpha2)
        return continent_code
    except (LookupError, KeyError, AttributeError):
        return "Unknown"

# Register the UDF for PySpark
get_continent_code_udf = F.udf(get_continent_code)

# Step 1: Load cleaned customers data
customers_path = os.path.join(silver_base_path, "cleaned", "customers", date_str)
log_message(f"Loading customers data from: {customers_path}")
df_customers_cleaned = spark.read.parquet(customers_path)
log_message(f"Loaded customers data with {df_customers_cleaned.count()} rows.")

# Step 2: Load cleaned sales data
sales_path = os.path.join(silver_base_path, "cleaned", "sales", date_str)
log_message(f"Loading sales data from: {sales_path}")
df_sales_cleaned = spark.read.parquet(sales_path)
log_message(f"Loaded sales data with {df_sales_cleaned.count()} rows.")

# Step 3: Enrich customers data with continent code
log_message("Enriching customers data with continent code.")
df_customers_enriched = df_customers_cleaned.withColumn("code_region", get_continent_code_udf(F.col("Country")))

log_message("Completed enriching customers data with continent code.")

# Step 4: Enrich sales data with continent code based on ShipCountry
log_message("Enriching sales data with continent code based on ShipCountry.")
df_sales_enriched = df_sales_cleaned.withColumn("region_code", get_continent_code_udf(F.col("ShipCountry")))
log_message("Completed enriching sales data with continent code.")

# Step 5: Add TotalAmount column to sales data
log_message("Calculating TotalAmount for sales data.")
df_sales_enriched = df_sales_enriched.withColumn(
    "TotalAmount",
    F.col("UnitPrice") * F.col("Quantity") * (1 - F.col("Discount"))
)
log_message("Added TotalAmount column to sales data.")

# Step 6: Calculate total purchase amount for each client
log_message("Calculating total purchase amount for each client.")
df_total_purchase = df_sales_enriched.groupBy("CustomerID").agg(
    F.sum("TotalAmount").alias("total_purchase_amount")
)
log_message(f"Aggregated total purchase amount for {df_total_purchase.count()} clients.")

# Step 7: Join customers data with total purchase amount
log_message("Joining customers data with total purchase amounts.")
df_customers_with_purchase = df_customers_enriched.join(
    df_total_purchase,
    on="CustomerID",
    how="left"
).fillna({"total_purchase_amount": 0})  # Fill missing values with 0
log_message(f"Joined customers data with total purchase amounts.")

# Step 8: Define client status based on total purchase amount
log_message("Defining client status based on total purchase amount.")
df_customers_final = df_customers_with_purchase.withColumn(
    "status_client",
    F.when(F.col("total_purchase_amount") > 10000, "VIP")
     .when((F.col("total_purchase_amount") >= 1000) & (F.col("total_purchase_amount") <= 10000), "Regular")
     .otherwise("Inactive")
)
log_message("Client statuses assigned based on purchase amount.")

# Select only the desired columns (existing columns + added columns)
columns_to_save = df_customers_cleaned.columns + ["code_region", "status_client"]

df_customers_final = df_customers_final.select(*columns_to_save)

# Step 9: Load cleaned products data
products_path = os.path.join(silver_base_path, "cleaned", "products", date_str)
log_message(f"Loading products data from: {products_path}")
df_products_cleaned = spark.read.parquet(products_path)
log_message(f"Loaded products data with {df_products_cleaned.count()} rows.")

# Step 10: Define product status based on adjusted logic
log_message("Assigning product status based on stock and order data.")
df_products_with_status = df_products_cleaned.withColumn(
    "product_status",
    F.when(F.col("Discontinued") == 1, "Discontinued")  # Discontinued products
     .when(F.col("UnitsInStock") < 10, "Low Stock")  # Low stock threshold
     .when((F.col("UnitsInStock") > 0) | (F.col("UnitsOnOrder") > 0), "Active")  # Active if stock or orders exist
     .otherwise("Inactive")  # Default to inactive
)
log_message("Product statuses assigned.")

# Save enriched products data with product status
enriched_products_path = os.path.join(silver_base_path, "enrichment", "products", date_str)
log_message(f"Saving enriched products data to: {enriched_products_path}")
df_products_with_status.write.mode("overwrite").parquet(enriched_products_path)
log_message("Enriched products data saved.")

enriched_customers_path = os.path.join(silver_base_path, "enrichment", "customers", date_str)
log_message(f"Saving enriched customers data to: {enriched_customers_path}")
df_customers_final.write.mode("overwrite").parquet(enriched_customers_path)
log_message("Enriched customers data saved.")

# Save enriched sales data with region code
enriched_sales_path = os.path.join(silver_base_path, "enrichment", "sales", date_str)
log_message(f"Saving enriched sales data to: {enriched_sales_path}")
df_sales_enriched.write.mode("overwrite").parquet(enriched_sales_path)
log_message("Enriched sales data saved.")

# Log message for completion
log_message(f"Data enrichment process completed successfully.")

# Stop Spark session
spark.stop()
