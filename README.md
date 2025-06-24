# ETL_Midterm_Rujwauka_463

# ETL Pipeline for Sales Order Data

## Project Overview
This project implements a full ETL (Extract, Transform, Load) pipeline to process sales order data from CSV files. The pipeline:

1. Extracts raw and incremental datasets
2. Transforms the data through cleaning, enrichment, and formatting
3. Loads the final datasets into both a SQLite database and Parquet files for downstream usage

---

## ETL Phases

### 1. Extract
- **Files loaded**:
  - `raw_data.csv`: Contains historical sales data
  - `incremental_data.csv`: Contains newly added records
  - Enables full and incremental data processing for up-to-date analytics.
  - Initial inspection of data (e.g., structure, column types)
  - Documentation of data quality issues

---

### 2. Transform

#### Missing Values
-  Replace nulls in critical fields
-  Prevent errors during processing and ensure consistent results
  - Text fields like `customer_name` and `region`: fill with `"Unknown"` or `"Unspecified"`
  - Numeric fields like `quantity` and `unit_price`: fill with column median
  - Dates: fill with placeholder `"1900-01-01"`

#### Duplicate Removal
-  Drop duplicate rows based on `order_id`
-  Ensure each order is uniquely processed

#### Enrichment
  - Calculate `total_price = quantity × unit_price`
  - Extract `order_month` and `order_year` from `order_date`
-  Adds analytical value for monthly/yearly reporting and sales metrics

#### Data Type Conversion
-  Ensure `quantity` and `order_id` are integers; convert `order_date` to datetime
-  Guarantee schema consistency for storage and querying

#### Price Tier Categorization (optional)
-  Add a column classifying `total_price` into tiers (e.g., Low, Medium, High)
-  Enables segmentation for pricing strategies or marketing analysis

---

### 3. Load

#### Output Formats
- Parquet Files:
  - `transformed/transformed_full.parquet`
  - `transformed/transformed_incremental.parquet`

#### Verification
- Load and print sample data from final outputs to verify integrity

---

## Tools & Technologies
- Python 3
- Pandas – data manipulation
- PyArrow – efficient columnar storage (Parquet format)
- Jupyter Notebooks – interactive ETL step execution

---
