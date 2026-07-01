from pyspark import pipelines as dp
from pyspark.sql.functions import upper, current_timestamp, sum

# Bronze Layer
@dp.table(
    name="bronze_orders01"
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
    name="silver_orders01"
)
def silver_orders():

    return (
        spark.readStream.table("bronze_orders01")
            .filter("amount > 500")
            .withColumn("city", upper("city"))
            .withColumn("ingestion_time", current_timestamp())
    )


# Gold Layer
@dp.table(
    name="gold_customer_sales01"
)
def gold_customer_sales():

    return (
        spark.read.table("silver_orders01")
            .groupBy("customer")
            .agg(sum("amount").alias("total_sales"))
    )
