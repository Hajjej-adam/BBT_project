from pyspark.sql import SparkSession
from datetime import datetime
import os
import logging
from pyspark.sql.functions import col, isnan, count, when
import re  # Import regular expression for identifying column name patterns
import json  # Import JSON for writing the audit report in JSON format

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Ingestion and Audit") \
    .getOrCreate()

# Define paths
base_path = "data/raw/"
bronze_base_path = "output/bronze/"
date_str = datetime.now().strftime("%Y-%m-%d")

# Log path organized by date
log_dir = os.path.join("logs", "data_processing", date_str)
log_path = os.path.join(log_dir, "data_processing.log")
report_path = os.path.join(log_dir, "audit_report.txt")
json_report_path = os.path.join(log_dir, "audit_report.json")

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
    json_report = {
        "source": source_name,
        "missing_values": {},
        "duplicate_rows": 0,
        "duplicate_columns": [],
        "data_type_and_format_inconsistencies": []
    }

    # 1. Check for missing values
    missing_values = df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns])
    missing_values_dict = missing_values.collect()[0].asDict()
    json_report["missing_values"] = missing_values_dict
    report.append(f"Missing Values:\n{missing_values_dict}\n")

    # 2. Check for duplicates (rows)
    duplicate_count = df.count() - df.dropDuplicates().count()
    json_report["duplicate_rows"] = duplicate_count
    report.append(f"Duplicate Rows: {duplicate_count}\n\n")

    # 3. Check for column name duplicates (based on patterns)
    column_names = df.columns
    base_names = {}
    duplicate_columns = []

    for column in column_names:
        base_name_match = re.match(r"([A-Za-z]+)(\d+)$", column)
        if base_name_match:
            base_name = base_name_match.group(1)
            if base_name in base_names:
                base_names[base_name].append(column)
            else:
                base_names[base_name] = [column]
    
    for base_name, columns in base_names.items():
        if len(columns) > 1:
            duplicate_columns.append(f" - {base_name}: {', '.join(columns)}")
    
    json_report["duplicate_columns"] = duplicate_columns
    if duplicate_columns:
        report.append("Duplicate Columns (Based on Similar Names):\n" + "\n".join(duplicate_columns))
    else:
        report.append("No duplicate columns found.\n")
    
    # 4. Check for data type and format inconsistencies
    def add_error_report(column, filter_condition, expected_type_description):
        """Helper function to add error details to the report."""
        error_rows = df.filter(filter_condition).select(id_column).collect()
        error_count = len(error_rows)
        if error_count > 0:
            error_ids = [row[id_column] for row in error_rows]
            json_report["data_type_and_format_inconsistencies"].append({
                "column": column,
                "error_count": error_count,
                "expected_type": expected_type_description,
                "error_ids": error_ids
            })
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
        add_error_report("Address", col("Address").isNull() | (col("Address") == ""), "must be a non-empty string")
        add_error_report("Country", ~col("Country").rlike("^[a-zA-Z ]+$"), "must contain only alphabetic characters and spaces")
        if source_name == "suppliers":
            add_error_report("SupplierID", (col("SupplierID").cast("int").isNull()) | (col("SupplierID") <= 0), "positive integer")
    elif source_name == "products":
        # Products table validation
        add_error_report("ProductID", (col("ProductID").cast("int").isNull()) | (col("ProductID") <= 0), "positive integer")
        add_error_report("SupplierID", (col("SupplierID").cast("int").isNull()) | (col("SupplierID") <= 0), "positive integer")
        add_error_report("CategoryID", (col("CategoryID").cast("int").isNull()) | (col("CategoryID") <= 0), "positive integer")
        add_error_report("UnitsInStock", (col("UnitsInStock").cast("int").isNull()) | (col("UnitsInStock") <= 0), "positive integer")
        add_error_report("UnitsOnOrder", (col("UnitsOnOrder").cast("int").isNull()) | (col("UnitsOnOrder") <= 0), "positive integer")
        add_error_report("ReorderLevel", (col("ReorderLevel").cast("int").isNull()) | (col("ReorderLevel") <= 0), "positive integer")
        add_error_report("UnitPrice", (col("UnitPrice").cast("float").isNull()) | (col("UnitPrice") <= 0), "positive float")
    
    report.append("\n" + "="*40 + "\n\n")
    return "\n".join(report), json_report

# Define sources and their respective ID columns for error reporting
sources = [
    ("sales", "OrderId0"),
    ("customers", "CustomerID"),
    ("products", "ProductID"),
    ("suppliers", "SupplierID")
]

# Process each source file
with open(report_path, "w") as report_file, open(json_report_path, "w") as json_report_file:
    json_report_file.write("[\n")  # Start of JSON array

    for idx, (source, id_column) in enumerate(sources):
        try:
            bronze_path = os.path.join(bronze_base_path, source, date_str)
            
            log_message(f"Starting data checking for {source} from {bronze_path} ")
            
            # Read data from Bronze layer (Parquet format)
            df = spark.read.parquet(bronze_path)
           

            # Write the audit report to the text file
            audit_report, json_report = audit_data(df, source, id_column)
            report_file.write(audit_report)
            report_file.write("\n" + "="*80 + "\n\n")
            
            # Write each JSON report to the JSON file
            json.dump(json_report, json_report_file, indent=4)
            if idx < len(sources) - 1:
                json_report_file.write(",\n")  # Separate each JSON object with a comma
            else:
                json_report_file.write("\n")  # No comma after the last JSON object

            log_message(f"Finished data checking of {source} ")
        
        except Exception as e:
            log_message(f"Error processing {source}: {str(e)}", level="error")

    json_report_file.write("]\n")  # End of JSON array
