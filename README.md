# Medallion Pipeline TPCH

## Overview
This project demonstrates a complete medallion architecture for the TPC-H dataset on Databricks using three layers: bronze, silver, and gold.

## Architecture
- **Bronze**: Ingests raw data as append-only tables from the TPC-H sample set.
- **Silver**: Cleans and conforms bronze outputs. Customer and orders use Auto CDC (SCD1), others use streaming tables with enforced quality rules.
- **Gold**: Produces business-ready aggregated tables for analytics and reporting.

## Project Structure
```
medallion-pipeline-tpch/
├── bronze.py
├── silver.py
├── gold.py
├── contracts.md
├── ai_rules.md
├── README.md
```

## Layer and Table Summary
- **Bronze Layer Tables:**
  - `customer_raw`: 750,000 rows, 10 columns
  - `orders_raw`: 7,500,000 rows, 11 columns
  - `lineitem_raw`: 29,999,795 rows, 18 columns
  - `part_raw`: 1,000,000 rows, 11 columns
  - `supplier_raw`: 50,000 rows, 9 columns
  - `partsupp_raw`: 4,000,000 rows, 7 columns
  - `nation_raw`: 25 rows, 6 columns
  - `region_raw`: 5 rows, 5 columns

- **Silver Layer Tables:**
  - See `bronze`, plus cleaning and data quality rules. (Row counts match bronze except for DQ rules.)

- **Gold Layer Tables:**
  - `gold_monthly_revenue_by_region`: 400 rows, 4 columns
  - `gold_customer_lifetime_value`: 750,000 rows, 8 columns (quartiles: Platinum, Gold, Silver, Bronze)
  - `gold_top_products`: 1,000,000 rows, 6 columns

## How to Run
1. Deploy the three pipeline files (bronze.py, silver.py, gold.py) as Spark Declarative Pipelines in Databricks.
2. Follow contract/data quality rules in contracts.md and ai_rules.md.
3. Activate (or schedule) pipeline jobs to keep all layers up to date.
4. Analyze/report using results from gold tables.

## Metrics (Verified)
- Total revenue (all gold tables): 1,089,835,179,247.22
- Value segments (customer CLV): Platinum 187,500, Gold 187,500, Silver 187,500, Bronze 187,500
- Gold layer: 5 regions x 80 months in gold_monthly_revenue_by_region

---
For full logic/contracts, see individual scripts and markdown files.
