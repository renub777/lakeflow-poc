from pyspark import pipelines as dp
from pyspark.sql.functions import upper, current_timestamp, sum

# Bronze Layer
@dp.table(
    name="bronze_orders"
)
def bronze_orders():

    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load("/Volumes/demo_schema/default/demo/source/")
    )


# Silver Layer
@dp.table(
    name="silver_orders"
)
def silver_orders():

    return (
        spark.readStream.table("bronze_orders")
            .filter("amount > 300")
            .withColumn("city", upper("city"))
            .withColumn("ingestion_time", current_timestamp())
    )


# Gold Layer
@dp.table(
    name="gold_customer_sales"
)
def gold_customer_sales():

    return (
        spark.read.table("silver_orders")
            .groupBy("customer")
            .agg(sum("amount").alias("total_sales"))
    )
