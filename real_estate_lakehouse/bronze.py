import pyspark.pipelines as dp
from pyspark.sql.functions import col, current_timestamp


@dp.table(name="csv_houses_madrid")
def csv_houses_madrid():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header","true")
        .load("/Volumes/re_dev/bronze/data_inputs/initial_load/")
    )

@dp.table(name="events_price_changed")
def events_price_changed():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","json")
        .option("cloudFiles.schemaHints","event_ts TIMESTAMP, listing_id INTEGER, old_price INTEGER, new_price INTEGER")
        .load("s3://real-estate-lakehouse-s3/landing/events/price_changed/")
        .select("*", col("_metadata.file_path").alias("_source_file"), current_timestamp().alias("_ingested_at"))
    )

