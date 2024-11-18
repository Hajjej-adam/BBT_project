from datetime import datetime
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import FloatType
import requests

# Get current date for the log directory
current_date = datetime.now().strftime("%Y-%m-%d")
log_dir = os.path.join("logs", "data_enrichment", current_date)

# Create log directory if it doesn't exist
os.makedirs(log_dir, exist_ok=True)

# Log file path
log_file = os.path.join(log_dir, "total_amount_calculation.log")

# Initialize logging
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
def log_message(message, level="info"):
    """Logs messages with the specified level."""
    if level == "info":
        logging.info(message)
    elif level == "error":
        logging.error(message)
    print(message)  # Also print to console for real-time feedback

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Efficient Tax Rate Calculation") \
    .getOrCreate()

# Sample data path
silver_base_path = "output/silver/"
date_str = datetime.now().strftime("%Y-%m-%d")

sales_path = os.path.join(silver_base_path, "cleaned", "sales", date_str)
enriched_sales_path = os.path.join(silver_base_path, "enrichment", "sales", date_str)

# API Key
TAXRATE_API_KEY = "J3R6HyH9qLGfrDAx5O66nmNauh8ckav73BkK53nitgKU7B2W0vdJG4n7mfa7S7kp"

# Function to fetch tax rate from API
def fetch_tax_rate(country, postal_code=None):
    if country == "USA" and postal_code:
        api_url = f"https://www.taxrate.io/api/v1/rate/getratebyzip?api_key={TAXRATE_API_KEY}&zip={postal_code}"
    else:
        api_url = f"https://www.taxrate.io/api/v1/rate/getratebyzip?api_key={TAXRATE_API_KEY}&zip={country}"
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            tax_data = response.json()
            log_message(f"Tax rate fetched for {country}-{postal_code}: {tax_data.get('rate_pct', 0.0)}")
            return tax_data.get("rate_pct", 0.0)
        else:
            log_message(f"Failed to fetch tax rate for {country}-{postal_code}. Returning 0.0")
            return 0.0
    except Exception as e:
        log_message(f"Error fetching tax rate for {country}-{postal_code}: {e}")
        return 0.0

# Load sales data
df_sales_cleaned = spark.read.parquet(sales_path)

# Extract unique countries and postal codes
unique_destinations = df_sales_cleaned.select("ShipCountry", "ShipPostalCode").distinct().collect()

# Group countries and postal codes
tax_rates_dict = {}
for row in unique_destinations:
    country = row["ShipCountry"]
    postal_code = row["ShipPostalCode"]
    if country == "USA":
        if postal_code not in tax_rates_dict:
            tax_rates_dict[postal_code] = fetch_tax_rate(country, postal_code)
    else:
        if country not in tax_rates_dict:
            tax_rates_dict[country] = fetch_tax_rate(country)

# Broadcast the tax rates dictionary
tax_rates_broadcast = spark.sparkContext.broadcast(tax_rates_dict)

# UDF to fetch tax rate from broadcast variable
@udf(FloatType())
def get_tax_rate_udf(country, postal_code):
    if country == "USA":
        return tax_rates_broadcast.value.get(postal_code, 0.0)
    else:
        return tax_rates_broadcast.value.get(country, 0.0)

# Apply tax rate and calculate total amount
df_sales_with_tax = df_sales_cleaned.withColumn(
    "TaxRate",
    when(get_tax_rate_udf(col("ShipCountry"), col("ShipPostalCode")).isNull(), 0.0)
    .otherwise(get_tax_rate_udf(col("ShipCountry"), col("ShipPostalCode")))
).withColumn(
    "total_amount",
    (col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) * (1 + col("TaxRate"))
)

# Save enriched data
df_sales_with_tax.write.mode("overwrite").parquet(enriched_sales_path)

# Log success
log_message(f"Enriched sales data saved to {enriched_sales_path}")
log_message(f"Tax rates dictionary: {tax_rates_dict}")

# Stop Spark session
spark.stop()
