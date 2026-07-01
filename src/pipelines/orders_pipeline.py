from pyspark import pipelines as dp
from pyspark.sql.functions import upper, current_timestamp, sum

# Bronze Layer
@dp.table(
    name="bronze_orders02"
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
    name="silver_orders02"
)
def silver_orders():

    return (
        spark.readStream.table("bronze_orders02")
            .filter("amount > 500")
            .withColumn("city", upper("city"))
            .withColumn("ingestion_time", current_timestamp())
    )


# Gold Layer
@dp.table(
    name="gold_customer_sales02"
)
def gold_customer_sales():

    return (
        spark.read.table("silver_orders02")
            .groupBy("customer")
            .agg(sum("amount").alias("total_sales"))
    )
