import os
from datetime import datetime
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load Gold Data to DW") \
    .config("spark.driver.extraClassPath", "C:/spark-3.4.3/sqljdbc_4.2.8112.200_enu/sqljdbc_4.2/enu/jre8/sqljdbc42.jar") \
    .getOrCreate()

# JDBC Configuration FOR SQL Server
jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=dw_bbt;integratedSecurity=true"
jdbc_properties = {
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Gold data paths
gold_base_path = "output/gold/"
date_str = datetime.now().strftime("%Y-%m-%d")

def load_to_sql_jdbc(table_name, df):
    """
    Loads data from a PySpark DataFrame into a SQL Server table using JDBC.
    Args:
        table_name (str): Name of the target SQL Server table.
        df (pyspark.sql.DataFrame): DataFrame containing the data to load.
    """
    try:
        df.write \
            .mode("append") \
            .jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)
        print(f"Data successfully loaded into {table_name}")
    except Exception as e:
        print(f"Error loading data into {table_name}: {e}")
        raise

# Load dimension tables
print("Loading dimension tables...")
df_customers = spark.read.parquet(os.path.join(gold_base_path, "dim_customers", date_str))
load_to_sql_jdbc("DimCustomer", df_customers)

df_products = spark.read.parquet(os.path.join(gold_base_path, "dim_products", date_str))
load_to_sql_jdbc("DimProduct", df_products)

df_store = spark.read.parquet(os.path.join(gold_base_path, "dim_store", date_str))
load_to_sql_jdbc("DimStore", df_store)

df_taxrate = spark.read.parquet(os.path.join(gold_base_path, "dim_taxrate", date_str))
load_to_sql_jdbc("DimTaxRate", df_taxrate)

df_exchange = spark.read.parquet(os.path.join(gold_base_path, "dim_exchange", date_str))
load_to_sql_jdbc("DimExchange", df_exchange)

df_calendar = spark.read.parquet(os.path.join(gold_base_path, "dim_calendar", date_str))
load_to_sql_jdbc("DimCalendar", df_calendar)

# Load fact table
print("Loading fact table...")
df_sales = spark.read.parquet(os.path.join(gold_base_path, "fact_sales", date_str))
load_to_sql_jdbc("FactSales", df_sales)

# Stop Spark session
spark.stop()
