import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from pyspark.sql.types import DateType
from pyspark.sql import Row

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Transform and Save Gold Data with Surrogate Keys") \
    .getOrCreate()

# Paths
silver_base_path = "output/silver/"
gold_base_path = "output/gold/"
date_str = datetime.now().strftime("%Y-%m-%d")

# Ensure Gold directory structure
os.makedirs(gold_base_path, exist_ok=True)

def save_gold_data(df, table_name):
    """
    Saves transformed data into the Gold directory.
    Args:
        df: Spark DataFrame to save.
        table_name: Name of the table (e.g., dim_customers).
    """
    path = os.path.join(gold_base_path, table_name, date_str)
    try:
        df.write.mode("overwrite").parquet(path)
        print(f"Saved {table_name} to {path}")
    except Exception as e:
        print(f"Error saving {table_name}: {e}")
        raise

# Step 1: Transform and Save Dimension Tables with Surrogate Keys


# DimTaxRate (with Surrogate Key)
tax_path = os.path.join(silver_base_path, "enrichment/taxrate", date_str)
df_tax = spark.read.parquet(tax_path)

df_tax_gold = df_tax.select(
    F.monotonically_increasing_id().alias("TaxRateKey"),  # Generate surrogate key
    F.col("TaxRateID").alias("TaxRateID"),
    F.col("Country").alias("Country"),
    F.col("Year").alias("Year"),
    F.col("TaxRate").alias("TaxRate")
)

save_gold_data(df_tax_gold, "dim_taxrate")


# DimExchange (with Surrogate Key)
exchange_path = os.path.join(silver_base_path, "enrichment/exchange_data", date_str)
df_exchange = spark.read.parquet(exchange_path)

df_exchange_gold = df_exchange.select(
    F.monotonically_increasing_id().alias("ExchangeKey"),  # Generate surrogate key
    F.col("ExchangeID").alias("ExchangeID"),
    F.col("date").alias("date"),
    F.col("country").alias("country"),
    F.col("currency").alias("currency"),
    F.col("exchange_rate_to_euro").alias("exchange_rate_to_euro")
)

save_gold_data(df_exchange_gold, "dim_exchange")



# DimCustomer (SCD Type 2 with ClientValue and Surrogate Key)
customers_path = os.path.join(silver_base_path, "enrichment/customers", date_str)
df_customers = spark.read.parquet(customers_path)

df_customers_gold = df_customers.select(
    F.monotonically_increasing_id().alias("CustomerKey"),  # Generate surrogate key
    F.col("CustomerID").alias("CustomerID"),
    F.col("CompanyName").alias("CompanyName"),
    F.col("ContactName").alias("ContactName"),
    F.col("ContactTitle").alias("ContactTitle"),
    F.col("Address").alias("Address"),
    F.col("City").alias("City"),
    F.col("Region").alias("Region"),
    F.col("PostalCode").alias("PostalCode"),
    F.col("Country").alias("Country"),
    F.col("Phone").alias("Phone"),
    F.col("Fax").alias("Fax"),
    F.col("code_region").alias("CodeRegion"),
    F.when(F.col("status_client") == "VIP", "High")
     .when(F.col("status_client") == "Regular", "Medium")
     .otherwise("Low").alias("ClientValue"),
    F.lit(True).alias("IsCurrent"),
    F.current_date().alias("EffectiveStartDate"),
    F.lit(None).cast("date").alias("EffectiveEndDate")
)

save_gold_data(df_customers_gold, "dim_customers")

# DimProduct (SCD Type 2 with Surrogate Key)
products_path = os.path.join(silver_base_path, "enrichment/products", date_str)
df_products = spark.read.parquet(products_path)

sales_path = os.path.join(silver_base_path, "enrichment/sales/with_currency_id", date_str)
df_sales = spark.read.parquet(sales_path)

df_last_sold = df_sales.groupBy("ProductID").agg(
    F.max("OrderDate").alias("LastSoldDate")
)

df_products_with_sales = df_products.join(
    df_last_sold,
    df_products["ProductID"] == df_last_sold["ProductID"],
    "left"
).select(
    df_products["*"],
    F.col("LastSoldDate")
)

df_products_gold = df_products_with_sales.withColumn(
    "ProductStatus",
     F.when(F.col("product_status") == "Discontinued", "Obsolete")
     .when((F.col("UnitsInStock") == 0) & (F.col("UnitsOnOrder") == 0) & 
           (F.datediff(F.lit("2024-01-01"), F.col("LastSoldDate")) > 365), "Obsolete")
     .when((F.col("product_status") == "Low Stock") | 
           (F.datediff(F.lit("2024-01-01"), F.col("LastSoldDate")) > 180), "End of Lifecycle")
     .otherwise("Active")
).select(
    F.monotonically_increasing_id().alias("ProductKey"),  # Surrogate key
    F.col("ProductID").alias("ProductID"),
    F.col("ProductName").alias("ProductName"),
    F.col("SupplierID").alias("SupplierID"),
    F.col("CategoryID").alias("CategoryID"),
    F.col("QuantityPerUnit").alias("QuantityPerUnit"),
    F.col("UnitPrice").alias("UnitPrice"),
    F.col("UnitsInStock").alias("UnitsInStock"),
    F.col("UnitsOnOrder").alias("UnitsOnOrder"),
    F.col("ReorderLevel").alias("ReorderLevel"),
    F.col("Discontinued").alias("Discontinued"),
    F.col("ProductStatus"),
    F.lit(True).alias("IsCurrent"),
    F.lit("2024-01-01").alias("EffectiveStartDate"),
    F.lit(None).cast("date").alias("EffectiveEndDate")
)

save_gold_data(df_products_gold, "dim_products")

# DimStore (Surrogate Key and Attractiveness Index)
suppliers_path = os.path.join(silver_base_path, "cleaned/suppliers", date_str)
df_suppliers = spark.read.parquet(suppliers_path)

df_products_suppliers = df_products.join(
    df_suppliers,
    df_products["SupplierID"] == df_suppliers["SupplierID"],
    "inner"
).select(
    df_suppliers["SupplierID"].alias("StoreID"),
    df_suppliers["CompanyName"].alias("StoreName"),
    df_suppliers["Address"].alias("Address"),
    df_suppliers["City"].alias("City"),
    df_suppliers["PostalCode"].alias("PostalCode"),
    df_suppliers["Country"].alias("Country")
)

df_sales_with_stores = df_sales.join(
    df_products_suppliers,
    df_sales["ProductID"] == df_products_suppliers["StoreID"],
    "inner"
)

#df_store_attractiveness = df_sales_with_stores.groupBy("StoreID").agg(
 #   F.count("*").alias("TotalTransactions"),
  #  F.sum("total_amount_in_euro").alias("TotalRevenue"),
  #  (F.sum("total_amount_in_euro") / F.count("*")).alias("AttractivenessIndex"))

df_store_gold = df_products_suppliers.select(
    "StoreID",
    "StoreName",
    "Address",
    "City",
    "PostalCode",
    "Country",
).distinct()
df_store_gold=df_store_gold.withColumn("StoreKey", F.monotonically_increasing_id())


save_gold_data(df_store_gold, "dim_store")
# The following code is an alternative way to create the DimCalendar with Surrogate Key
# df_date = df_sales.select(
#     F.col("OrderDate").alias("CalendarDate"),
#     F.dayofweek("OrderDate").alias("DayOfWeek"),
#     F.month("OrderDate").alias("Month"),
#     F.year("OrderDate").alias("Year"),
#     F.quarter("OrderDate").alias("Quarter")
# ).distinct()

# Add surrogate key for Calendar dimension
# df_calendar_gold = df_date.withColumn(
#     "CalendarKey",
#     F.monotonically_increasing_id()
# )

# save_gold_data(df_calendar_gold, "dim_calendar")
# Generate a list of dates between 2022-01-01 and 2025-01-01
start_date = "2022-01-01"
end_date = "2025-01-01"

# Create a DataFrame with all dates in the range
df_dates = spark.sql(f"""
    SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS CalendarDate
""")

# Add calendar attributes with date in YYYY-MM-DD format
df_calendar = df_dates.select(
    F.date_format(F.col("CalendarDate"), "yyyy-MM-dd").alias("CalendarDate"),
    F.dayofweek("CalendarDate").alias("DayOfWeek"),
    F.month("CalendarDate").alias("Month"),
    F.year("CalendarDate").alias("Year"),
    F.quarter("CalendarDate").alias("Quarter")
)

# Add surrogate key using monotonically increasing ID
df_calendar_gold = df_calendar.withColumn(
    "CalendarKey",
    F.monotonically_increasing_id()
)

# Save the DataFrame to the gold layer (assuming save_gold_data function exists)
save_gold_data(df_calendar_gold, "dim_calendar")

# FactSales (Map Natural Keys to Surrogate Keys)
# Join Sales with DimCustomer
df_fact_sales = df_sales.join(
    df_customers_gold.select("CustomerID", "CustomerKey"),
    "CustomerID",
    "inner"
)
# Join with DimCalendar
df_fact_sales = df_fact_sales.join(
    df_calendar_gold.select("CalendarDate", "CalendarKey"),
    df_fact_sales["OrderDate"] == df_calendar_gold["CalendarDate"],
    "inner"
)
# Join with DimTaxRate
df_fact_sales = df_fact_sales.join(
    df_tax_gold.select("TaxRateKey", "TaxRateID"),
    df_fact_sales["TaxRateID"] == df_tax_gold["TaxRateID"],
    "inner"
)

# Join with DimExchange
df_fact_sales = df_fact_sales.join(
    df_exchange_gold.select("ExchangeKey", "ExchangeID"),
    df_fact_sales["ExchangeID"] == df_exchange_gold["ExchangeID"],
    "inner"
)

# Join with DimProduct + DimStore
df_products_with_store = df_products_gold.join(
    df_store_gold,
    df_products_gold["SupplierID"] == df_store_gold["StoreID"],
    "left"
).select("ProductID", "ProductKey", "StoreKey")
df_products_with_store

df_fact_sales = df_fact_sales.join(
    df_products_with_store,
    "ProductID",
    "left"
).filter(F.col("StoreKey").isNotNull())



# Final FactSales
df_fact_sales = df_fact_sales.select(
    F.monotonically_increasing_id().alias("SalesID"),
    F.col("CustomerKey"),
    F.col("ProductKey"),
    F.col("StoreKey"),
    F.col("CalendarKey"),
    F.col("OrderID"),
    F.col("OrderDate"),
    F.col("ShippedDate"),
    F.col("region_code").alias("Region"),
    F.col("Quantity"),
    F.col("Discount"),
    F.col("TaxRateKey"),
    F.col("ExchangeKey")
)

save_gold_data(df_fact_sales, "fact_sales")

# Stop Spark session
spark.stop()
