"""These are the sql performed throughout project."""

"""table creation for Gold database"""

###dim_customer
CREATE TABLE dim_customer (
  customer_id INT,
  customer_name STRING,
  city STRING,
  state STRING
)
STORED AS PARQUET;

###dim_product
CREATE TABLE dim_product (
  product_id INT,
  product_name STRING,
  category STRING,
  unit_price INT
)
STORED AS PARQUET;

###dim_store
CREATE TABLE dim_store (
  store_id INT,
  store_name STRING,
  city STRING,
  state STRING
)
STORED AS PARQUET;

####dim_date
CREATE TABLE dim_date (
  date_key DATE,
  year INT,
  month INT,
  day INT
)
STORED AS PARQUET;

###GOLD FACT TABLE

###Fact_sales
sale_id,product_id,customer_id,store_id,sale_date,quantity
CREATE TABLE fact_sales (
  sale_id INT,
  customer_id INT,
  product_id INT,
  store_id INT,
  date_key DATE,
  quantity INT,
  total_amount INT
)
PARTITIONED BY (date_key)
STORED AS PARQUET;



"""SQL EXECUTIONS:"
SELECT product_name, SUM(total_amount)
FROM fact_sales f
JOIN dim_product p
ON f.product_id = p.product_id
GROUP BY product_name;



 SELECT COUNT(*) FROM bronze_sales;

 SELECT COUNT(*) FROM silver_sales;

 SELECT COUNT(*) FROM fact_sales;

 SELECT * FROM silver_sales LIMIT 10;

 SELECT * FROM fact_sales LIMIT 10;

 """Total Revenue by Product"""
SELECT p.product_name, SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.product_name;

"""Sales by Store"
SELECT s.store_name, SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
GROUP BY s.store_name;

