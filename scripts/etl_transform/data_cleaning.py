from pyspark.sql import SparkSession, functions as F
import json
import os

# Initialize Spark session
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()
    
def clean_data(df, json_report, id_column):
    """
    Cleans the DataFrame based on inconsistencies found in the JSON audit report.

    Parameters:
        df (DataFrame): The original DataFrame to clean.
        json_report (dict): The audit report in JSON format for the specific source.
        id_column (str): The primary key column for identifying records.

    Returns:
        DataFrame: A cleaned version of the original DataFrame.
    """

    for inconsistency in json_report.get("data_type_and_format_inconsistencies", []):
        column = inconsistency["column"]
        expected_type = inconsistency["expected_type"]
        error_ids = inconsistency["error_ids"]

        # Filter rows with errors
        error_df = df.filter(df[id_column].isin(error_ids))

        # Apply data type conversions and cleaning based on expected type
        if "positive integer" in expected_type:
            df = df.withColumn(column, F.when(df[column].cast("int").isNotNull(), df[column].cast("int")))
        elif "positive float" in expected_type:
            df = df.withColumn(column, F.when(df[column].cast("float").isNotNull(), df[column].cast("float")))
        elif "numeric with . ( ) - symbols" in expected_type:
            df = df.withColumn(column, F.regexp_replace(df[column], r"[^\d\.\(\)\-]", ""))
            # Further validation and cleaning can be added based on specific requirements
        elif "must be a non-empty string" in expected_type:
            df = df.withColumn(column, F.when(df[column].isNull() | (df[column] == ""), None).otherwise(df[column]))

    # Handle missing values (after type conversions)
    df = df.fillna({column: None for column, _ in json_report.get("missing_values", {}).items()})

    # Handle duplicate rows
    df = df.dropDuplicates()

    # Handle duplicate columns (renaming if necessary)
    for duplicate_set in json_report.get("duplicate_columns", []):
        for idx, column in enumerate(duplicate_set[1:], start=1):
            df = df.withColumnRenamed(column, f"{column}_dup{idx}")

    return df
# Read the JSON audit report
json_report_path = "logs/data_ingestion/<date>/audit_report.json"  # Adjust path as necessary
with open(json_report_path, "r") as json_file:
    audit_reports = json.load(json_file)  # Assuming audit reports is a list for multiple sources

# Process each data source based on its audit report
for report in audit_reports:
    source_name = report["source"]
    id_column = {"sales": "OrderId0", "customers": "CustomerID", "products": "ProductID", "suppliers": "SupplierID"}[source_name]
    bronze_path = os.path.join(bronze_base_path, source_name, date_str)
    silver_path = os.path.join(silver_base_path, source_name, date_str)
    
    # Load the original data from the Bronze layer
    df = spark.read.parquet(bronze_path)
    
    # Clean the data based on the JSON report
    cleaned_df = clean_data(df, report, id_column)
    
    # Write the cleaned data to the Silver layer
    os.makedirs(silver_path, exist_ok=True)
    cleaned_df.write.mode("overwrite").parquet(silver_path)
    
    print(f"Cleaned data for {source_name} written to {silver_path}")
