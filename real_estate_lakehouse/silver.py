import pyspark.pipelines as dp
from pyspark.sql.functions import col, current_timestamp

@dp.table(name="re_dev.silver.price_changes")
@dp.expect("no_rescued_data","_rescued_data IS NULL")
@dp.expect_or_drop("positive_new_price","new_price > 0")
@dp.expect_or_drop("has_listing_id","listing_id IS NOT NULL")
@dp.expect_or_fail("has_event_id","event_id IS NOT NULL")
def price_changes():
    return (
        spark.readStream
        .table("events_price_changed")
        .dropDuplicates(["event_id"])
    )