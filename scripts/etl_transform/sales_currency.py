from datetime import datetime
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, udf
import pycountry
import requests

# Get current date for the log directory
current_date = datetime.now().strftime("%Y-%m-%d")
log_dir = os.path.join("logs", "data_enrichment", current_date)

# Create log directory if it doesn't exist
os.makedirs(log_dir, exist_ok=True)

# Log file path
log_file = os.path.join(log_dir, "currency_exchange_rate_calculation.log")

# Initialize logging
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def log_message(message, level="info"):
    """Logs messages with the specified level."""
    if level == "info":
        logging.info(message)
    elif level == "error":
        logging.error(message)
    print(message)  # Also print to console for real-time feedback

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Currency Exchange Rate Calculation") \
    .getOrCreate()

# Sample data path
silver_base_path = "output/silver/"
date_str = datetime.now().strftime("%Y-%m-%d")

sales_path = os.path.join(silver_base_path, "enrichment", "sales", date_str)
enriched_sales_path = os.path.join(silver_base_path, "enrichment", "sales", "with_currency",date_str)

# API Key for currency exchange rates
EXCHANGE_API_KEY = "cur_live_llnllJMpxMUec8hyGOvsMbcF78718Mjh4GKq2mMO"

# Initialize a dictionary to store exchange rates
exchange_rates_dict = {}

# List of Eurozone countries (ISO-3166 country names)
eurozone_countries = [
    "Austria", "Belgium", "Cyprus", "Estonia", "Finland", "France", 
    "Germany", "Greece", "Ireland", "Italy", "Latvia", "Lithuania", 
    "Luxembourg", "Malta", "Netherlands", "Portugal", "Slovakia", 
    "Slovenia", "Spain"
]

# Function to fetch exchange rate from API
def fetch_exchange_rate(from_currency, to_currency):
    log_message(f"Attempting to fetch exchange rate from {from_currency} to {to_currency}")
    api_url = f"https://api.currencyapi.com/v3/latest?apikey={EXCHANGE_API_KEY}&currencies={to_currency}&base_currency={from_currency}"

    try:
        response = requests.get(api_url)
        log_message(f"API Response for {from_currency} to {to_currency}: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            if 'data' in data and to_currency in data['data']:
                exchange_rate = data['data'][to_currency]['value']
                log_message(f"Exchange rate fetched for {from_currency}-{to_currency}: {exchange_rate}")
                return exchange_rate
            else:
                log_message(f"Exchange rate data missing for {from_currency}-{to_currency}.")
        else:
            log_message(f"Failed to fetch exchange rate for {from_currency}-{to_currency}. Status code: {response.status_code}")
    except Exception as e:
        log_message(f"Error fetching exchange rate for {from_currency}-{to_currency}: {e}")

    return None

# Function to handle API calls and cache the result
def get_exchange_rate(from_currency, to_currency):
    if (from_currency, to_currency) not in exchange_rates_dict:
        # Fetch and cache the rate if not already cached
        exchange_rate = fetch_exchange_rate(from_currency, to_currency)
        if exchange_rate is not None:
            exchange_rates_dict[(from_currency, to_currency)] = exchange_rate
    return exchange_rates_dict.get((from_currency, to_currency), 0.0)

# Updated get_currency_code function
def get_currency_code(country_name_or_code):
    try:
        # Specific cases for Venezuela, Brazil, and the UK
        if country_name_or_code.title() == "Venezuela":
            return 'VES'
        elif country_name_or_code.title() == "Brazil":
            return 'BRL'
        elif country_name_or_code.title() == "UK":
            return 'GBP'
        # First, attempt to match by exact country name
        country = pycountry.countries.get(name=country_name_or_code.title())
        
        # If not found, try matching by alpha-2 or alpha-3 code
        if not country:
            country = pycountry.countries.get(alpha_2=country_name_or_code.upper())
        if not country:
            country = pycountry.countries.get(alpha_3=country_name_or_code.upper())
        
        # Check if it's a Euro country
        if country_name_or_code.title() in eurozone_countries:
            return 'EUR'

        # If country is found, look up the currency using country.numeric
        if country:
            currency = pycountry.currencies.get(numeric=country.numeric)
            if currency:
                return currency.alpha_3  # Return ISO 4217 currency code
            else:
                return None

    except AttributeError:
        print(f"Could not find currency for country: {country_name_or_code}")
    return None

# UDF to get the currency based on the ShipCountry
@udf
def get_currency_udf(country):
    return get_currency_code(country)

# Load sales data
df_sales_cleaned = spark.read.parquet(sales_path)

# Add the currency column based on ShipCountry
df_sales_with_currency = df_sales_cleaned.withColumn(
    "Currency",
    get_currency_udf(col("ShipCountry"))
)

# Ensure all necessary exchange rates are loaded before UDF execution
unique_currencies = df_sales_with_currency.select("Currency").distinct().rdd.flatMap(lambda x: x).collect()

for currency in unique_currencies:
    if currency != "EUR":  # No need to fetch rates for EUR
        get_exchange_rate("EUR", currency)  # Preload exchange rate for EUR -> currency

log_message("Preloaded exchange rates.")

# Broadcast the exchange rates dictionary
exchange_rates_broadcast = spark.sparkContext.broadcast(exchange_rates_dict)

# UDF to fetch exchange rate from broadcast variable and convert
@udf
def get_exchange_rate_udf(from_currency, to_currency):
    return exchange_rates_broadcast.value.get((from_currency, to_currency), 0.0)

# Calculate converted total_amounts
df_sales_with_exchange_rate = df_sales_with_currency.withColumn(
    "Converted_amount",
    when(col("Currency") == "EUR", col("total_amount"))  # No conversion if currency is already EUR
    .otherwise(col("total_amount") * get_exchange_rate_udf(lit("EUR"), col("Currency")))  # Convert from EUR to the destination currency
)

# Show a sample of the resulting dataframe to check if everything is working
df_sales_with_exchange_rate.show(5)
# UDF for currency conversion to include currency symbol in the amount
@udf
def format_amount_with_currency(amount, currency):
    if amount is None:
        return None
    return f"{amount} {currency}"

# Updated logic for 'Converted_amount' to handle EUR as is and add currency
df_sales_with_exchange_rate = df_sales_with_currency.withColumn(
    "Converted_amount",
    when(col("Currency") == "EUR", col("total_amount"))  # No conversion if currency is EUR
    .otherwise(col("total_amount") * get_exchange_rate_udf(lit("EUR"), col("Currency")))  # Convert if not EUR
)

# Add currency symbol to the 'Converted_amount' (e.g., "255 USD")
df_sales_with_exchange_rate = df_sales_with_exchange_rate.withColumn(
    "Converted_amount_with_currency",
    format_amount_with_currency(col("Converted_amount"), col("Currency"))
)

# Show the resulting dataframe
df_sales_with_exchange_rate.select("Converted_amount", "Converted_amount_with_currency").show(5)

# Save enriched data without saving the 'Currency' column
df_sales_with_exchange_rate.drop("Currency","Converted_amount").write.mode("overwrite").parquet(enriched_sales_path)

# Log success

# Log success
log_message(f"Enriched sales data with exchange rate saved to {enriched_sales_path}")
log_message(f"Exchange rates dictionary: {exchange_rates_dict}")

# Stop Spark session
spark.stop()
