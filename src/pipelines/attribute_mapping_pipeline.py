from pyspark import pipelines as dp

@dp.table(
    name="attribute_mapping_bronze"
)
def attribute_mapping_bronze():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/checkpoints/attribute_mapping/schema"
        )
        .option("header", "true")
        .load(
            "abfss://marketingbdlmetadatacontainer@dbstorageda22d80080adls
.dfs.core.windows.net/source-folder/"
        )
    )
``
