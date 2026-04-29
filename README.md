

# iPhone Sales Analytics Platform

## Overview

This project builds an end-to-end data pipeline to analyze iPhone sales across customers, stores, and time using **HDFS, Hive, and PySpark**. It follows the **Medallion Architecture (Bronze → Silver → Gold)** and implements a **Star Schema** for analytics.

---

## Architecture

```
CSV Files → HDFS → Bronze → Silver → Gold
```

* **Bronze Layer**: Raw data ingestion (CSV → Parquet)
* **Silver Layer**: Data cleaning, type casting, partitioning
* **Gold Layer**: Dimensional modeling (Fact + Dimension tables)

---

## Tech Stack

* Storage: HDFS
* Processing: PySpark
* Data Warehouse: Hive
* File Format: Parquet
* Modeling: Star Schema

---

## Data Sources

* `customers.csv`
* `products.csv`
* `stores.csv`
* `sales.csv`

---

## Data Model (Star Schema)

* **Fact Table**: `fact_sales`
* **Dimension Tables**:

  * `dim_customer`
  * `dim_product`
  * `dim_store`
  * `dim_date`

---

##  Pipeline Steps

### 1. Bronze Layer

* Read raw CSV from HDFS
* Store as Parquet in Hive tables
  (`bronze_customers`, `bronze_sales`, etc.)

### 2. Silver Layer

* Clean data and enforce schema
* Convert data types
* Partition sales by date
  (`silver_sales`)

### 3. Gold Layer

* Create dimension tables
* Build `fact_sales` using joins
* Calculate `total_amount = quantity × unit_price`
* Partition by date for performance

---


## Key Features

* Medallion Architecture implementation
* Partitioned fact table for performance
* Parquet format for efficient storage
* Scalable data pipeline using PySpark

---

## Conclusion

This project demonstrates how to build a scalable data engineering pipeline using modern big data tools, transforming raw data into analytics-ready insights.

---
