from datetime import datetime
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, when
from pyspark.sql.types import FloatType

# Initialize Logging
current_date = datetime.now().strftime("%Y-%m-%d")
log_dir = os.path.join("logs", "data_enrichment", current_date)
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "total_amount_calculation.log")
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def log_message(message, level="info"):
    if level == "info":
        logging.info(message)
    elif level == "error":
        logging.error(message)
    print(message)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Tax Rate Calculation") \
    .getOrCreate()

# Paths
silver_base_path = "output/silver/"
date_str = datetime.now().strftime("%Y-%m-%d")
sales_path = os.path.join(silver_base_path, "enrichment", "sales", date_str)
enriched_sales_path = os.path.join(silver_base_path, "enrichment", "sales","total_amount", date_str)
tax_rates_path = "data/external/taxRates.csv"

# Load tax rates and sales data
df_tax_rates = spark.read.csv(tax_rates_path, header=True, inferSchema=True)
df_sales_cleaned = spark.read.parquet(sales_path)

# Extract year from OrderDate
df_sales_cleaned = df_sales_cleaned.withColumn("OrderYear", year(col("OrderDate")))

# Reshape tax rates
df_tax_rates_long = df_tax_rates.select(
    col("Country"),
    col("2022").alias("rate_2022").cast(FloatType()),
    col("2023").alias("rate_2023").cast(FloatType()),
    col("2024").alias("rate_2024").cast(FloatType())
)

# Debugging: Verify reshaped tax rates
log_message("Tax rates sample:")
df_tax_rates_long.show()

# Join sales data with tax rates on Country
df_sales_with_tax = df_sales_cleaned.join(
    df_tax_rates_long,
    df_sales_cleaned["ShipCountry"] == df_tax_rates_long["Country"],
    "left"
)

# Assign tax rate based on OrderYear, default to 0 if ShipCountry or OrderDate is unavailable
df_sales_with_tax = df_sales_with_tax.withColumn(
    "TaxRate",
    when((col("ShipCountry").isNull()) | (col("OrderYear").isNull())| (col("ShipCountry")=="Unknown"), 0.0)
    .when(col("OrderYear") == 2022, col("rate_2022"))
    .when(col("OrderYear") == 2023, col("rate_2023"))
    .when(col("OrderYear") == 2024, col("rate_2024"))
    .otherwise(0.0)
)

# Check for mismatches
mismatched_rows = df_sales_with_tax.filter(col("TaxRate").isNull()).count()
if mismatched_rows > 0:
    log_message(f"WARNING: {mismatched_rows} rows have null TaxRate. Investigating...", level="error")
    df_sales_with_tax.filter(col("TaxRate").isNull()).show()

# Calculate total amount
df_sales_with_tax = df_sales_with_tax.withColumn(
    "total_amount",
    (col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) * (1 + col("TaxRate"))
)

# Validate rows with null total_amount
null_total_amount_rows = df_sales_with_tax.filter(col("total_amount").isNull()).count()
if null_total_amount_rows > 0:
    log_message(f"WARNING: {null_total_amount_rows} rows have null total_amount.", level="error")
    df_sales_with_tax.filter(col("total_amount").isNull()).show()

# Drop unwanted columns before saving
columns_to_drop = ["Country", "rate_2022", "rate_2023", "rate_2024"]
df_sales_with_tax = df_sales_with_tax.drop(*columns_to_drop)

# Save enriched sales data
df_sales_with_tax.write.mode("overwrite").parquet(enriched_sales_path)
log_message(f"Enriched sales data saved to {enriched_sales_path}")

# Stop Spark Session
spark.stop()
