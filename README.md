# ETL_Midterm_Rujwauka_463

# ETL Pipeline for Sales Order Data

## Project Overview
This project implements a complete **ETL (Extract, Transform, Load)** pipeline designed to process and manage sales order data efficiently. The pipeline is built using Python and enables robust data cleaning, enrichment, and loading into both **SQLite** and **Parquet** formats for flexible downstream use.

---

## ETL Workflow

### 1. Extract
- Reads input files: `raw_data.csv` and `incremental_data.csv`
- Performs initial data inspection
- Notes data quality issues (e.g., missing or inconsistent values)

### 2. Transform
- Cleans and standardizes data:
  - Fills missing values
  - Removes duplicate entries
  - Converts data types (e.g., strings to datetime, floats to integers)
- Enriches data with:
  - Calculated fields (`total_price`, `order_month`, `order_year`)
  - Categorization fields (e.g., future option: `price_tier`)
  
### 3. Load
- Saves the processed dataset in:
  - Parquet files (`transformed/` and `loaded/` directories)
- Validates saved files with sample output previews

---

## Tech Stack

- Python 3
- pandas: data manipulation
- numpy: numerical operations
- pyarrow: Parquet file operations

---
