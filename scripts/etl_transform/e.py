from datetime import datetime
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when,udf
import pycountry



# Initialize Logging
current_date = datetime.now().strftime("%Y-%m-%d")
log_dir = os.path.join("logs", "data_enrichment", current_date)
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "currency_id_assignment.log")
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def log_message(message, level="info"):
    if level == "info":
        logging.info(message)
    elif level == "error":
        logging.error(message)
    print(message)

# Helper function to convert country names to ISO codes
@udf
def country_to_iso_code(country_name):
    try:
        if country_name.lower() in ["uk", "united kingdom"]:
            country_name = "United Kingdom"
        return pycountry.countries.lookup(country_name).alpha_3
    except LookupError:
        return None

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Currency ID Assignment") \
    .getOrCreate()

# Paths
silver_base_path = "output/silver/"
date_str = datetime.now().strftime("%Y-%m-%d")
sales_path = os.path.join(silver_base_path, "enrichment", "sales", "with_tax_rate_id", date_str)
products_path = os.path.join(silver_base_path, "enrichment", "products", date_str)
suppliers_path = os.path.join(silver_base_path, "cleaned", "suppliers", date_str)
exchange_rate_path = os.path.join(silver_base_path, "enrichment", "exchange_data", date_str)
enriched_sales_path = os.path.join(silver_base_path, "enrichment", "sales", "with_currency_id", date_str)

# Load datasets
df_sales = spark.read.parquet(sales_path)
df_products = spark.read.parquet(products_path)
df_suppliers = spark.read.parquet(suppliers_path)
df_exchange_rates = spark.read.parquet(exchange_rate_path)

# Debugging: Log schemas
log_message("Products Schema:")
df_products.printSchema()
log_message("Suppliers Schema:")
df_suppliers.printSchema()
log_message("Exchange Rates Schema:")
df_exchange_rates.printSchema()



# Join products and suppliers to get product-country mapping
df_product_country = df_products.join(
    df_suppliers,
    df_products["SupplierID"] == df_suppliers["SupplierID"],
    "inner"
).select(
    df_products["ProductID"],
    df_suppliers["Country"].alias("ProductCountry")
)

# Convert `ProductCountry` to ISO codes
df_product_country = df_product_country.withColumn(
    "ProductCountryISO",
    country_to_iso_code(col("ProductCountry"))
)

# Add ISO country information to sales data
df_sales = df_sales.join(
    df_product_country,
    df_sales["ProductID"] == df_product_country["ProductID"],
    "left"
).drop(df_product_country["ProductID"])

# Join sales data with exchange rates
log_message("Joining sales data with exchange rates to assign CurrencyID.")
df_sales_with_currency_id = df_sales.join(
    df_exchange_rates,
    (df_sales["ProductCountryISO"] == df_exchange_rates["country"]) &
    (df_sales["OrderDate"] == df_exchange_rates["date"]),
    "left"
)



# Log rows where `CurrencyID` is null after the join
log_message("Rows with Null CurrencyID After Join:")
df_sales_with_currency_id.filter(col("ExchangeID").isNull()).show()

# Drop unwanted columns
columns_to_drop = ["currency", "date", "ProductCountry", "country", "ProductCountryISO"]
df_sales_with_currency_id = df_sales_with_currency_id.drop(*columns_to_drop)
df_sales_with_currency_id = df_sales_with_currency_id.dropDuplicates()

# Save enriched sales data with CurrencyID
log_message(f"Saving enriched sales data with CurrencyID to: {enriched_sales_path}")
df_sales_with_currency_id.write.mode("overwrite").parquet(enriched_sales_path)
log_message("Enriched sales data with CurrencyID saved successfully.")

# Stop Spark session
spark.stop()
