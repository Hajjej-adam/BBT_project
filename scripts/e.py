from datetime import datetime
import json
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, regexp_replace, to_date

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Cleaning Based on Audit Report") \
    .getOrCreate()

# Define paths
base_path = "data/raw/"
bronze_base_path = "output/bronze/"
silver_base_path = "output/silver/"
date_str = datetime.now().strftime("%Y-%m-%d")

# Log path organized by date
log_dir = os.path.join("logs", "data_processing", date_str)
log_path = os.path.join(log_dir, "data_processing.log")
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

# Mapping of sources to their respective ID columns
source_id_mapping = {
    "sales": "OrderId",
    "customers": "CustomerID",
    "products": "ProductID",
    "suppliers": "SupplierID"
}

# Function to clean a DataFrame based on its audit report
def clean_data(df, report, source_name):
    """
    Cleans the DataFrame based on the audit report.

    Parameters:
        df (DataFrame): The DataFrame to clean.
        report (dict): The audit report for the DataFrame.
        source_name (str): The name of the data source.

    Returns:
        DataFrame: The cleaned DataFrame.
    """
    id_column = source_id_mapping[source_name]
    
    log_message(f"Starting cleaning for {source_name}")

    # 1. Handle Missing Values
    missing_values = report.get("missing_values", {})
    fill_values = {}
    for column, count_missing in missing_values.items():
        if count_missing > 0:
            if column in ["ShipRegion", "ShipPostalCode", "ShipCountry", "CompanyName", "City", "PostalCode", "Fax", "Phone"]:
                fill_values[column] = "Unknown"
                log_message(f"Filling missing values in '{column}' with 'Unknown'")
            elif column in ["ShippedDate", "OrderDate", "RequiredDate"]:
                fill_values[column] = None  # Keep as null
                log_message(f"Filling missing values in '{column}' with null")
            elif column in ["Freight", "UnitPrice", "Discount"]:
                fill_values[column] = 0.0
                log_message(f"Filling missing values in '{column}' with 0.0")
            elif column == "Quantity":
                fill_values[column] = 1
                log_message(f"Filling missing values in '{column}' with 1")
            else:
                fill_values[column] = "Unknown"  # Default fallback

    # Remove any None values from the dictionary
    fill_values = {k: v for k, v in fill_values.items() if v is not None}

    if fill_values:
        df = df.fillna(fill_values)
        log_message(f"Applied fillna with values: {fill_values}")

    # 2. Remove Duplicate Rows (already handled in Silver layer, but double-check)
    duplicate_rows = report.get("duplicate_rows", 0)
    if duplicate_rows > 0:
        df = df.dropDuplicates()
        log_message(f"Removed {duplicate_rows} duplicate rows")

    # 3. Remove Duplicate Columns
    duplicate_columns = report.get("duplicate_columns", [])
    for dup_set in duplicate_columns:
        try:
            # Example format: " - OrderID: OrderID0, OrderID14"
            # We need to parse the string to extract column names
            _, columns_str = dup_set.split(":")
            columns = [col.strip() for col in columns_str.split(",")]
            
            # Keep the first column, drop the rest
            first_column = columns[0]
            columns_to_drop = columns[1:]
            
            # Drop duplicate columns
            df = df.drop(*columns_to_drop)
            log_message(f"Dropped duplicate columns {columns_to_drop} from '{dup_set.split(':')[0].strip()}'")
            
            # Remove numeric suffix from the first column name, if it has one (e.g., "OrderID0" becomes "OrderID")
            if first_column[-1].isdigit():
                base_column_name = first_column.rstrip("0123456789")
                df = df.withColumnRenamed(first_column, base_column_name)
                log_message(f"Renamed column '{first_column}' to '{base_column_name}'")
        
        except Exception as e:
            log_message(f"Error parsing duplicate columns '{dup_set}': {str(e)}", level="error")


    # 4. Handle Data Type and Format Inconsistencies
    inconsistencies = report.get("data_type_and_format_inconsistencies", [])
    for inconsistency in inconsistencies:
        column = inconsistency.get("column")
        expected_type = inconsistency.get("expected_type")
        error_ids = inconsistency.get("error_ids", [])

        if not column or not expected_type:
            continue  # Skip if necessary information is missing

        log_message(f"Fixing inconsistencies in column '{column}' for records {error_ids}")

        if "positive integer" in expected_type:
            # Replace non-positive integers with 1
            df = df.withColumn(column, when(col(column) <= 0, 1).otherwise(col(column)))
            log_message(f"Set non-positive values in '{column}' to 1")

        elif "positive float" in expected_type:
            # Replace negative floats with 0.0
            df = df.withColumn(column, when(col(column) < 0, 0.0).otherwise(col(column)))
            log_message(f"Set negative values in '{column}' to 0.0")

        elif "numeric with . ( ) - symbols" in expected_type:
            # Standardize phone/fax formats by removing invalid characters
            # Alternatively, set to 'Unknown' if invalid
            # Here, we'll set to 'Unknown' for invalid formats
            df = df.withColumn(column, when(col(column).rlike(r"^[0-9.\-\(\) ]+$"), col(column)).otherwise("Unknown"))
            log_message(f"Standardized formats in '{column}', set invalid formats to 'Unknown'")

        elif "must be a non-empty string" in expected_type:
            # Replace empty strings with 'Unknown'
            df = df.withColumn(column, when((col(column) == "") | col(column).isNull(), "Unknown").otherwise(col(column)))
            log_message(f"Set empty strings in '{column}' to 'Unknown'")

        else:
            log_message(f"Unhandled expected type '{expected_type}' for column '{column}'", level="warning")

    log_message(f"Finished cleaning for {source_name}")
    return df

# Read the JSON audit report
with open(json_report_path, "r") as json_file:
    try:
        audit_reports = json.load(json_file)  # This should be a list of audit report dictionaries
        log_message(f"Successfully loaded JSON audit report from '{json_report_path}'")
    except json.JSONDecodeError as e:
        log_message(f"Error decoding JSON audit report: {str(e)}", level="error")
        spark.stop()
        exit(1)

# Iterate through each audit report and clean the corresponding data
for report in audit_reports:
    source = report.get("source")
    if not source:
        log_message(f"Missing 'source' in audit report: {report}", level="error")
        continue

    id_column = source_id_mapping.get(source)
    if not id_column:
        log_message(f"No ID column mapping found for source '{source}'", level="error")
        continue

    bronze_path = os.path.join(bronze_base_path, source, date_str)
    silver_path = os.path.join(silver_base_path, source, date_str)

    log_message(f"Starting cleaning process for '{source}'")

    # Check if bronze path exists
    if not os.path.exists(bronze_path):
        log_message(f"bronze path does not exist for '{source}': {bronze_path}", level="error")
        continue

    try:
        # Read data from the bronze layer
        df_bronze = spark.read.parquet(bronze_path)
        log_message(f"Loaded data for '{source}' from '{bronze_path}'")

        # Clean the data based on the audit report
        df_cleaned = clean_data(df_bronze, report, source)

        # Ensure the silver path exists
        os.makedirs(silver_path, exist_ok=True)

        # Write the cleaned data to the silver layer
        df_cleaned.write.mode("overwrite").parquet(silver_path)
        log_message(f"Cleaned data for '{source}' written to '{silver_path}'")

    except Exception as e:
        log_message(f"Error cleaning data for '{source}': {str(e)}", level="error")
        continue

# Stop Spark session after cleaning
spark.stop()
log_message("Data cleaning process completed.")
