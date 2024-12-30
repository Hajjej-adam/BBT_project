# Data Warehouse for BBT

## Project Description

This project outlines an **Extract, Transform, Load (ETL)** process for data warehousing. It leverages Python scripts to:
- Extract data from various sources.
- Perform transformations to prepare the data.
- Load the processed data into a data warehouse or data lake.

---

## ETL Steps

### 1. Data Extraction (`etl_extract.py`)
- This script extracts data from **CSV files**.
- Ensure proper configuration of **connection details** and data retrieval logic.

### 2. Data Transformation (`etl_transformation/`)
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

### 3. Data Loading (`etl_load.py`)
- This script loads the transformed data into the target data warehouse or data lake.
- Ensure configuration of:
  - **Connection details**.
  - **Data insertion logic**.

---

## Data Warehouse Design Considerations

### 1. SCD Type 2 in Dimensions
- Supports historical tracking of dimension changes using:
  - `StartDate`
  - `EndDate`
  - `IsCurrent` columns.
- Example dimensions:
  - `DimCustomers`
  - `DimProducts`
- Allows for analyzing data at specific points in time.

### 2. Fact Table
- Holds Key Performance Indicators (**KPIs**) such as:
  - `AttractivenessIndex`
  - `CustomerValue`
  - `ProductSalesStatus`.

### 3. Date Dimension
- Enables filtering and aggregation based on various time periods.

### 4. Surrogate Keys
- Fact tables utilize surrogate keys (e.g., `SalesID`) to enhance:
  - **Performance**.
  - **Data integration**.

---

## Prerequisites

- **Python**: Recommended version `[Specify your preferred version]`.
- **Python libraries**: List all required libraries in `requirements.txt`.
- **Access to data sources**: Specify types and credentials.
- **SQL Server**: Ensure SQL Server is installed and running on your machine.
- **SQL Server Management Studio (SSMS)**: Recommended for managing and restoring the database.

---

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/Hajjej-adam/spark_project.git
    ```

2. Install the required Python libraries:

    ```bash
    pip install -r requirements.txt
    ```

---

## Restoring the `dw_bbt.bak` File in SQL Server

To restore the `dw_bbt.bak` file and load the database into SQL Server, follow these steps:

### Step 1: Open SQL Server Management Studio (SSMS)
1. Launch SSMS and connect to your SQL Server instance.

### Step 2: Restore the Database
1. **Right-click** on the **Databases** node in the Object Explorer.
2. Select **Restore Database...** from the context menu.

### Step 3: Configure the Restore Options
1. In the **Restore Database** window:
   - **Source**: Select "Device" and click the **...** button to browse for the backup file.
   - **Add**: Navigate to the location of the `dw_bbt.bak` file and select it.
   - **Destination**: 
     - **Database**: Enter `dw_bbt` as the database name.
2. Click **OK** to proceed.

### Step 4: Verify and Complete the Restore
1. Review the restore settings in the **General** and **Options** tabs.
2. Ensure that the **Restore** option is selected and click **OK** to start the restore process.
3. Once the restore is complete, you should see the `dw_bbt` database listed under the **Databases** node in SSMS.

---

## Usage

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

## Additional Notes
- **Code with caution**: Ensure the configurations and logic align with your specific project requirements before executing any scripts.
- For more details, refer to the individual script comments and documentation.
- **Backup and Restore**: Always ensure you have a backup of your database before performing any restore operations.
