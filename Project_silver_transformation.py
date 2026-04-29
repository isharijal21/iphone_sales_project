"""
Isha Rijal:
In is a silver transformation 

"""""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


#funtion for each data tables
def silver_customer_transform(spark):
    df = spark.table("iphone_analytics.bronze_customers")

    clean_df = (
        df.withColumn("customer_id", col("customer_id").cast("int"))
          .withColumn("customer_name", col("customer_name").cast("string"))
          .withColumn("city", col("city").cast("string"))
          .withColumn("state", col("state").cast("string"))
    )

    (
    clean_df.write.mode("overwrite").format("parquet") \
        .saveAsTable("iphone_analytics.silver_customers")
    )
    return "silver_customers"


def silver_products_transform(spark):
    df = spark.table("iphone_analytics.bronze_products")

    clean_df = (
        df.withColumn("product_id", col("product_id").cast("int"))
          .withColumn("unit_price", col("unit_price").cast("int"))
          .withColumn("product_name", col("product_name").cast("string"))
          .withColumn("category", col("category").cast("string"))
    )

    clean_df.write.mode("overwrite").format("parquet") \
        .saveAsTable("iphone_analytics.silver_products")
    return "silver_products"


def silver_stores_transform(spark):
    df = spark.table("iphone_analytics.bronze_stores")

    clean_df = (
        df.withColumn("store_id", col("store_id").cast("int"))
          .withColumn("store_name", col("store_name").cast("string"))
          .withColumn("city", col("city").cast("string"))
          .withColumn("state", col("state").cast("string"))
    )
    (
    clean_df.write.mode("overwrite").format("parquet") \
        .saveAsTable("iphone_analytics.silver_stores")
    )
    return "silver_stores"


def silver_sales_transform(spark):
    df = spark.table("iphone_analytics.bronze_sales")

    clean_df = (
        df.withColumn("sale_id", col("sale_id").cast("int"))
          .withColumn("product_id", col("product_id").cast("int"))
          .withColumn("customer_id", col("customer_id").cast("int"))
          .withColumn("store_id", col("store_id").cast("int"))
          .withColumn("quantity", col("quantity").cast("int"))
          .withColumn("sale_date", to_date(col("sale_date")))
    )

    (
        clean_df.write
        .mode("overwrite")
        .partitionBy("sale_date")
        .format("parquet")
        .saveAsTable("iphone_analytics.silver_sales")
    )
    return "silver_sales"



if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Silver Layer Transformation") \
        .enableHiveSupport() \
        .getOrCreate()

    silver_customer_transform(spark)
    silver_products_transform(spark)
    silver_stores_transform(spark)
    silver_sales_transform(spark)

