"""Isha Rijal:

In this project I work on performing a simple data ingestion . Data(i.e csv table)that is stored in the hdfs are ingested into the
iphone analytics database in Hive.Prior to running this cade I have created a directory in hdfs (i.e hdfs_iphone_data/)
I had also created iphone_analytics database in hive and proceeded on ingestion"""

from pyspark.sql import SparkSession
#function that ingest
def bronze_ingestion(spark, csv_path, table_name):
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csv_path)
    )
    (
        df.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(f"iphone_analytics.bronze_{table_name}")
    )
    return f"bronze_{table_name}"


#my hdfs data is stored in the file path below
if __name__ == "__main__":
    spark = SparkSession.builder.appName("iphone_sales").enableHiveSupport().getOrCreate()


    bronze_ingestion(spark,"hdfs_iphone_data/customers.csv","customers")
    bronze_ingestion(spark, "hdfs_iphone_data/products.csv", "products")
    bronze_ingestion(spark, "hdfs_iphone_data/sales.csv", "sales")
    bronze_ingestion(spark, "hdfs_iphone_data/stores.csv", "stores")




