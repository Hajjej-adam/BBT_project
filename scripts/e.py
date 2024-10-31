from pyspark.sql import SparkSession
from datetime import datetime
import os
import logging
from pyspark.sql.functions import col, isnan, count, when, collect_list

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Ingestion and Audit") \
    .getOrCreate()

# Define paths
base_path = "data/raw/"
bronze_base_path = "output/bronze/"
date_str = datetime.now().strftime("%Y-%m-%d")

# Log path organized by date
log_dir = os.path.join("logs", "data_ingestion", date_str)
log_path = os.path.join(log_dir, "data_ingestion.log")
report_path = os.path.join(log_dir, "audit_report.txt")

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
    print(message)  # Also print to console for real-time feedback

# Function to generate audit report with expected data type checks
def audit_data(df, source_name, id_column):
    report = []
    report.append(f"Audit Report for {source_name}\n")
    report.append("="*40 + "\n")

    # 1. Check for missing values
    report.append("Missing Values:\n")
    missing_values = df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns])
    missing_values_dict = missing_values.collect()[0].asDict()
    report.append(str(missing_values_dict) + "\n")
    
    # 2. Check for duplicates
    duplicate_count = df.count() - df.dropDuplicates().count()
    report.append(f"Duplicate Rows: {duplicate_count}\n\n")

    # 3. Check for data type and format inconsistencies based on source
    report.append("Data Type and Format Inconsistencies:\n")
    
    def add_error_report(column, filter_condition, expected_type_description):
        """Helper function to add error details to the report."""
        error_rows = df.filter(filter_condition).select(id_column).collect()
        error_count = len(error_rows)
        if error_count > 0:
            error_ids = [row[id_column] for row in error_rows]
            report.append(f" - {column}: {error_count} records do not match expected {expected_type_description} at {id_column}s {', '.join(map(str, error_ids))}\n")

    if source_name == "sales":
        # Sales table validation
        add_error_report("Freight", (col("Freight").cast("float").isNull()) | (col("Freight") < 0), "positive float")
        add_error_report("UnitPrice", (col("UnitPrice").cast("float").isNull()) | (col("UnitPrice") < 0), "positive float")
        add_error_report("Discount", (col("Discount").cast("float").isNull()) | (col("Discount") < 0), "positive float")
        add_error_report("OrderId0", (col("OrderId0").cast("int").isNull()) | (col("OrderId0") <= 0), "positive integer")
        add_error_report("EmployeeId", (col("EmployeeId").cast("int").isNull()) | (col("EmployeeId") < 0), "positive integer")
        add_error_report("ShipVia", (col("ShipVia").cast("int").isNull()) | (col("ShipVia") < 0), "positive integer")
        add_error_report("Quantity", (col("Quantity").cast("int").isNull()) | (col("Quantity") < 0), "positive integer")
        add_error_report("ProductId", (col("ProductId").cast("int").isNull()) | (col("ProductId") < 0), "positive integer")

    elif source_name == "customers" or source_name == "suppliers":
        # Common validation for Customers and Suppliers
        numeric_phone_fax_pattern = "^[0-9.()\\- ]*$"
        add_error_report("Phone", ~col("Phone").rlike(numeric_phone_fax_pattern), "numeric with . ( ) - symbols")
        add_error_report("Fax", ~col("Fax").rlike(numeric_phone_fax_pattern), "numeric with . ( ) - symbols")
       # Replace Address validation to allow any string
        add_error_report("Address", col("Address").isNull() | (col("Address") == ""), "must be a non-empty string")

        # Additional check for SupplierID in Suppliers
        if source_name == "suppliers":
            add_error_report("SupplierID", (col("SupplierID").cast("int").isNull()) | (col("SupplierID") <= 0), "positive integer")

    elif source_name == "products":
        # Products table validation
        add_error_report("ProductID", (col("ProductID").cast("int").isNull()) | (col("ProductID") <= 0), "positive integer")
        add_error_report("SupplierID", (col("SupplierID").cast("int").isNull()) | (col("SupplierID") <= 0), "positive integer")
        add_error_report("CategoryID", (col("CategoryID").cast("int").isNull()) | (col("CategoryID") <= 0), "positive integer")
        add_error_report("UnitsInStock", (col("UnitsInStock").cast("int").isNull()) | (col("UnitsInStock") < 0), "positive integer")
        add_error_report("UnitsOnOrder", (col("UnitsOnOrder").cast("int").isNull()) | (col("UnitsOnOrder") < 0), "positive integer")
        add_error_report("ReorderLevel", (col("ReorderLevel").cast("int").isNull()) | (col("ReorderLevel") < 0), "positive integer")
        add_error_report("UnitPrice", (col("UnitPrice").cast("float").isNull()) | (col("UnitPrice") <= 0), "positive float")

    report.append("\n" + "="*40 + "\n\n")
    return "\n".join(report)

# Define sources and their respective ID columns for error reporting
sources = [
    ("sales", "OrderId0"),
    ("customers", "CustomerID"),
    ("products", "ProductID"),
    ("suppliers", "SupplierID")
]

# Process each source file
with open(report_path, "w") as report_file:
    for source, id_column in sources:
        try:
            raw_path = os.path.join(base_path, f"{source}.csv")
            bronze_path = os.path.join(bronze_base_path, source, date_str)

            # Log start of the ingestion process for this source
            log_message(f"Starting ingestion for {source} from {raw_path} to {bronze_path}")

            # Read raw data
            df = spark.read.csv(raw_path, header=True, inferSchema=True)

            # Ensure the bronze path exists
            os.makedirs(bronze_path, exist_ok=True)

            # Write to Bronze layer in Parquet format, organized by date
            df.write.mode("overwrite").parquet(bronze_path)

            # Log successful ingestion
            log_message(f"Successfully ingested {source} data to {bronze_path}")

            # Generate and save audit report
            report = audit_data(df, source, id_column)
            report_file.write(report)

            log_message(f"Audit report for {source} generated and saved.")

        except Exception as e:
            # Log any errors encountered during ingestion
            log_message(f"Error ingesting {source} data: {e}", level="error")

# Stop Spark session
spark.stop()
log_message("Data ingestion and audit process completed.")
