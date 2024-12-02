# **Data Warehouse for BBT**

## **Project Description**

This project outlines an **Extract, Transform, Load (ETL)** process for data warehousing. It leverages Python scripts to:
- Extract data from various sources.
- Perform transformations to prepare the data.
- Load the processed data into a data warehouse or data lake.

---

## **ETL Steps**

### **1. Data Extraction (`etl_extract.py`)**
- This script extracts data from **CSV files**.
- Ensure proper configuration of **connection details** and data retrieval logic.

### **2. Data Transformation (`etl_transformation/`)**
This directory contains scripts for various data transformations:
- **`audit_report.py`**: Generates a report detailing potential data quality issues encountered during extraction.
- **`data_cleaning.py`**: Cleans and prepares data, including:
  - Handling missing values.
  - Resolving inconsistencies.
  - Formatting errors.
- **`add_columns.py`**: Adds new calculated columns or derived attributes based on business logic.
- **`sales_tax.py`**: Calculates and applies sales tax based on predefined rules or tax rates.
- **`sales_currency.py`**: Converts sales data to a consistent currency format.
- **`etl_gold/map_cols.py`**: 
  - Maps columns from the source data to the target data warehouse schema.
  - Ensures proper data alignment and understanding.

### **3. Data Loading (`etl_load.py`)**
- This script loads the transformed data into the target data warehouse or data lake.
- Ensure configuration of:
  - **Connection details**.
  - **Data insertion logic**.

---

## **Data Warehouse Design Considerations**

### **1. SCD Type 2 in Dimensions**
- Supports historical tracking of dimension changes using:
  - `StartDate`
  - `EndDate`
  - `IsCurrent` columns.
- Example dimensions:
  - `DimCustomers`
  - `DimProducts`
- Allows for analyzing data at specific points in time.

### **2. Fact Table**
- Holds Key Performance Indicators (**KPIs**) such as:
  - `AttractivenessIndex`
  - `CustomerValue`
  - `ProductSalesStatus`.

### **3. Date Dimension**
- Enables filtering and aggregation based on various time periods.

### **4. Surrogate Keys**
- Fact tables utilize surrogate keys (e.g., `SalesID`) to enhance:
  - **Performance**.
  - **Data integration**.

---

## **Prerequisites**

- **Python**: Recommended version `[Specify your preferred version]`.
- **Python libraries**: List all required libraries in `requirements.txt`.
- **Access to data sources**: Specify types and credentials.

---

## **Installation**

1. Clone the repository:

    ```bash
    git clone https://github.com/Hajjej-adam/spark_project.git
    ```

2. Install the required Python libraries:

    ```bash
    pip install -r requirements.txt
    ```

---

## **Usage**

1. Configure **connection details** in the relevant scripts:
   - Database credentials for **extraction**.
   - Target data warehouse connection for **loading**.

2. Review and customize the **data transformation logic** within the `etl_transformation/` scripts to meet your specific needs.

3. Run the ETL process by executing the scripts in sequence:

    ```bash
    python etl_extract.py
    python etl_transformation/data_cleaning.py  # Run other transformation scripts as needed
    python etl_load.py
    ```

---

## **Additional Notes**
- **Code with caution**: Ensure the configurations and logic align with your specific project requirements before executing any scripts.
- For more details, refer to the individual script comments and documentation.
