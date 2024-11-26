1-etl_extract.py
2-etl_transformation/audit_report.py
3-etl_transformation/data_cleaning.py
4-etl_transformation/add_columns.py
5-etl_transformation/sales_tax.py
6-etl_transformation/sales_currency.py

taxRate = 0 ::::> tax rate is unavailable

## SCD Type 2 in Dimensions:

The StartDate, EndDate, and IsCurrent columns in dimensions (DimCustomers, DimProducts) support historical tracking of changes.
Fact Table:

## All KPIs (e.g., AttractivenessIndex, CustomerValue, ProductSalesStatus) are included here.

Date Dimension:

This supports filtering and aggregation by various time periods.

## Surrogate Keys:

Fact table uses surrogate keys (SalesID) for better performance and integration.
