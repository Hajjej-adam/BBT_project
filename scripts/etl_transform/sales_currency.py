from datetime import datetime
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, when, lit, last
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType

# Initialize Logging
current_date = datetime.now().strftime("%Y-%m-%d")
log_dir = os.path.join("logs", "data_enrichment", current_date)
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "currency_conversion.log")
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def log_message(message, level="info"):
    if level == "info":
        logging.info(message)
    elif level == "error":
        logging.error(message)
    print(message)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Currency Conversion Using CSV") \
    .getOrCreate()

# Paths
silver_base_path = "output/silver/"
date_str = datetime.now().strftime("%Y-%m-%d")
sales_path = os.path.join(silver_base_path, "enrichment", "sales", "total_amount",date_str)
products_path = os.path.join(silver_base_path, "enrichment", "products", date_str)
suppliers_path = os.path.join(silver_base_path, "cleaned", "suppliers", date_str)
exchange_rate_path = "data/external/exchange_data.csv"
enriched_sales_path = os.path.join(silver_base_path, "enrichment", "sales", "with_currency", date_str)

# Load datasets
df_sales = spark.read.parquet(sales_path)
df_products = spark.read.parquet(products_path)
df_suppliers = spark.read.parquet(suppliers_path)
df_exchange_rates = spark.read.csv(exchange_rate_path, header=True, inferSchema=True)

# Debugging: Print schemas to verify
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
    df_products["ProductID"], df_suppliers["Country"].alias("ProductCountry")
)

# Add country information to sales data
df_sales = df_sales.join(
    df_product_country,
    df_sales["ProductID"] == df_product_country["ProductID"],
    "left"
).drop(df_product_country["ProductID"])


# Extract year from OrderDate for exchange rate matching
df_sales = df_sales.withColumn("OrderYear", year(col("OrderDate")))


# Prepare exchange rate data and fill missing rates
df_exchange_rates = df_exchange_rates.withColumn(
    "exchange_rate_to_euro",
    col("exchange_rate_to_euro").cast(FloatType())
)

# Define a window specification to get the last available rate
window_spec = Window.partitionBy("country").orderBy("date")

# Add a column with the last available exchange rate for each country
df_exchange_rates = df_exchange_rates.withColumn(
    "last_exchange_rate_to_euro",
    last("exchange_rate_to_euro", ignorenulls=True).over(window_spec)
)

log_message("Exchange Rates Data with Last Available Rate:")
df_exchange_rates.show()

# Join sales data with exchange rates
df_sales_with_exchange_rate = df_sales.join(
    df_exchange_rates,
    (df_sales["ProductCountry"] == df_exchange_rates["country"]) &
    (df_sales["OrderDate"] == df_exchange_rates["date"]),
    "left"
)

# Fill missing exchange rates with the last available rate
df_sales_with_exchange_rate = df_sales_with_exchange_rate.withColumn(
    "exchange_rate_to_euro",
    when(col("exchange_rate_to_euro").isNull(), col("last_exchange_rate_to_euro"))
    .otherwise(col("exchange_rate_to_euro"))
)

# Define Eurozone countries
eurozone_countries = ["France", "Italy", "Germany", "Austria", "Spain", "Portugal", "Netherlands",
                      "Finland", "Belgium", "Greece", "Ireland", "Slovakia", "Slovenia",
                      "Estonia", "Lithuania", "Latvia", "Luxembourg", "Malta"]

# Handle Eurozone countries and missing exchange rates
df_sales_with_exchange_rate = df_sales_with_exchange_rate.withColumn(
    "exchange_rate_to_euro",
    when(col("ProductCountry").isin(eurozone_countries), lit(1.0))  # Set to 1.0 for Eurozone countries
    .otherwise(col("exchange_rate_to_euro"))
)

# Log rows where exchange rate is null after the adjustment
log_message("Rows with Null Exchange Rate After Adjustment:")
df_sales_with_exchange_rate.filter(col("exchange_rate_to_euro").isNull()).show()

# Convert total_amount to Euros
df_sales_with_exchange_rate = df_sales_with_exchange_rate.withColumn(
    "total_amount_in_euro",
    col("total_amount") / col("exchange_rate_to_euro")
)


# Validate rows with missing converted amounts
missing_conversion_rows = df_sales_with_exchange_rate.filter(col("total_amount_in_euro").isNull()).count()
if missing_conversion_rows > 0:
    log_message(f"WARNING: {missing_conversion_rows} rows have null total_amount_in_euro.", level="error")
    df_sales_with_exchange_rate.filter(col("total_amount_in_euro").isNull()).show()

# Drop unwanted columns
columns_to_drop = ["currency", "date", "ProductCountry", "country","last_exchange_rate_to_euro"]
df_sales_with_exchange_rate = df_sales_with_exchange_rate.drop(*columns_to_drop)
df_sales_with_exchange_rate = df_sales_with_exchange_rate.dropDuplicates()

# Save enriched sales data
df_sales_with_exchange_rate.write.mode("overwrite").parquet(enriched_sales_path)
log_message(f"Enriched sales data with currency conversion saved to {enriched_sales_path}")

# Stop Spark session
spark.stop()
