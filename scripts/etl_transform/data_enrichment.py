from pyspark.sql import SparkSession
import logging
from pyspark.sql import functions as F
import pycountry
import pycountry_convert as pc
import os
from datetime import datetime


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Ingestion") \
    .getOrCreate()

# Define paths
base_path = "data/raw/"
bronze_base_path = "output/bronze/"
silver_base_path = "output/silver/"
date_str = datetime.now().strftime("%Y-%m-%d")

# Log path organized by date
log_dir = os.path.join("logs", "data_ingestion", date_str)
log_path = os.path.join(log_dir, "data_ingestion.log")

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

# UDF to convert country names to continent codes
def get_region_code(country_name):
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
        # Get the ISO-3166 alpha-2 country code
        country_code = pycountry.countries.lookup(country_name).alpha_2
        # Convert country code to continent code
        continent_code = pc.country_alpha2_to_continent_code(country_code)
        return continent_code
    except (LookupError, KeyError):
        return "Unknown"

# Register the UDF for use in PySpark
get_region_code_udf = F.udf(get_region_code)

# Paths
sales_path = os.path.join(silver_base_path, "sales", date_str)  # Updated to load from silver path
enriched_sales_path = os.path.join(silver_base_path, "sales", "enrichment", date_str)  # Path to save enriched data

# Load the cleaned sales data from the silver path
df_sales_cleaned = spark.read.parquet(sales_path)

# Enrich the DataFrame with the 'code_region' column based on 'ShipCountry'
df_sales_enriched = df_sales_cleaned.withColumn("code_region", get_region_code_udf(F.col("ShipCountry")))

# Save the enriched DataFrame with the new column
df_sales_enriched.write.mode("overwrite").parquet(enriched_sales_path)

# Log message for completion
log_message(f"Enriched sales data with 'code_region' saved to '{enriched_sales_path}'")
