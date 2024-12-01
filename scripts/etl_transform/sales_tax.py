from datetime import datetime
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, when, lit, udf
import pycountry

# Initialize Logging
current_date = datetime.now().strftime("%Y-%m-%d")
log_dir = os.path.join("logs", "data_enrichment", current_date)
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "tax_rate_id_assignment.log")
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def log_message(message, level="info"):
    if level == "info":
        logging.info(message)
    elif level == "error":
        logging.error(message)
    print(message)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Tax Rate ID Assignment") \
    .getOrCreate()

# Paths
silver_base_path = "output/silver/"
date_str = datetime.now().strftime("%Y-%m-%d")
sales_path = os.path.join(silver_base_path, "enrichment", "sales", date_str)
tax_rates_path = os.path.join(silver_base_path, "enrichment", "taxrate", date_str)
enriched_sales_path = os.path.join(silver_base_path, "enrichment", "sales", "with_tax_rate_id", date_str)
# UDF to convert country names to ISO codes
@udf
def get_iso_code(country_name):
    try:
        if country_name.lower() in ["uk", "united kingdom"]:
            country_name = "United Kingdom"
        return pycountry.countries.lookup(country_name).alpha_3
    except LookupError:
        return "UNK"  # Use "UNK" for unknown countries


# Load tax rates and sales data from Silver
log_message(f"Loading tax rates from: {tax_rates_path}")
df_tax_rates = spark.read.parquet(tax_rates_path)
log_message(f"Tax rates loaded with {df_tax_rates.count()} rows.")

log_message(f"Loading sales data from: {sales_path}")
df_sales_cleaned = spark.read.parquet(sales_path)
log_message(f"Sales data loaded with {df_sales_cleaned.count()} rows.")

# Convert Country Names to ISO Codes in Both DataFrames
log_message("Converting country names to ISO codes.")
df_tax_rates = df_tax_rates.withColumn("ISO_Country", get_iso_code(col("Country"))).drop("Country")
df_sales_cleaned = df_sales_cleaned.withColumn("ISO_ShipCountry", get_iso_code(col("ShipCountry")))

# Extract year from OrderDate in sales
df_sales_cleaned = df_sales_cleaned.withColumn("OrderYear", year(col("OrderDate")))

# Join sales data with tax rates on ISO_Country and OrderYear
log_message("Joining sales data with tax rates to assign TaxRateID.")
df_sales_with_tax = df_sales_cleaned.join(
    df_tax_rates,
    (df_sales_cleaned["ISO_ShipCountry"] == df_tax_rates["ISO_Country"]) &
    (df_sales_cleaned["OrderYear"] == df_tax_rates["Year"]),
    "left"
)

# Check for mismatched rows
mismatched_rows = df_sales_with_tax.filter(col("TaxRateID").isNull()).count()
if mismatched_rows > 0:
    log_message(f"WARNING: {mismatched_rows} rows have null TaxRateID. Investigating...", level="error")
    df_sales_with_tax.filter(col("TaxRateID").isNull()).show()

# Drop unwanted columns before saving
columns_to_drop = ["ISO_ShipCountry", "ISO_Country","Year","OrderYear", "TaxRate"]
df_sales_with_tax = df_sales_with_tax.drop(*columns_to_drop)

# Save enriched sales data with TaxRateID
log_message(f"Saving enriched sales data with TaxRateID to: {enriched_sales_path}")
df_sales_with_tax.write.mode("overwrite").parquet(enriched_sales_path)
log_message("Enriched sales data with TaxRateID saved successfully.")

# Stop Spark Session
spark.stop()
