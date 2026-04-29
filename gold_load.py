from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth


def load_dim_customer(spark):
    df = spark.table("iphone_analytics.silver_customers")

    df.write.mode("overwrite").format("parquet") \
        .saveAsTable("iphone_analytics.dim_customer")


def load_dim_product(spark):
    df = spark.table("iphone_analytics.silver_products")

    df.write.mode("overwrite").format("parquet") \
        .saveAsTable("iphone_analytics.dim_product")


def load_dim_store(spark):
    df = spark.table("iphone_analytics.silver_stores")

    df.write.mode("overwrite").format("parquet") \
        .saveAsTable("iphone_analytics.dim_store")


def load_dim_date(spark):
    df = spark.table("iphone_analytics.silver_sales")

    dim_date = (
        df.select(col("sale_date").alias("date_key"))
          .distinct()
          .withColumn("year", year(col("date_key")))
          .withColumn("month", month(col("date_key")))
          .withColumn("day", dayofmonth(col("date_key")))
    )

    dim_date.write.mode("overwrite").format("parquet") \
        .saveAsTable("iphone_analytics.dim_date")


def load_fact_sales(spark):
    sales = spark.table("iphone_analytics.silver_sales")
    products = spark.table("iphone_analytics.silver_products")

    fact_df = (
        sales.join(products, "product_id")
             .withColumn("total_amount", col("quantity") * col("unit_price"))
             .select(
                 "sale_id",
                 "customer_id",
                 "product_id",
                 "store_id",
                 col("sale_date").alias("date_key"),
                 "quantity",
                 "total_amount"
             )
    )

    (
        fact_df.write
        .mode("overwrite")
        .partitionBy("date_key")
        .format("parquet")
        .saveAsTable("iphone_analytics.fact_sales")
    )



if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Gold Layer Load") \
        .enableHiveSupport() \
        .getOrCreate()

    load_dim_customer(spark)
    load_dim_product(spark)
    load_dim_store(spark)
    load_dim_date(spark)
    load_fact_sales(spark)
    print("Gold Layer Load")