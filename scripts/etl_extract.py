from pyspark.sql import SparkSession
from datetime import datetime
import os
import logging

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Ingestion") \
    .getOrCreate()

# Define paths
base_path = "data/raw/"
bronze_base_path = "output/bronze/"
date_str = datetime.now().strftime("%Y-%m-%d")

# Log path organized by date
log_dir = os.path.join("logs", "data_ingestion", date_str)
log_path = os.path.join(log_dir, "data_ingestion.log")

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

# Define sources
sources = ["sales", "customers", "products", "suppliers"]

# Process each source file
for source in sources:
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
        
    except Exception as e:
        # Log any errors encountered during ingestion
        log_message(f"Error ingesting {source} data: {e}", level="error")

# Stop Spark session
spark.stop()
log_message("Data ingestion process completed.")
