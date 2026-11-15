import pyspark.pipelines as dp
from pyspark.sql import Column
from pyspark.sql import functions as F
from utilities.quality import failed_rules
from utilities.rules import PRICE_CHANGE_CRITICAL_RULES, PRICE_CHANGE_QUARANTINE_RULES, PRICE_CHANGE_WARNING_RULES


@dp.temporary_view(name="price_changes_evaluated")
@dp.expect_all({**PRICE_CHANGE_QUARANTINE_RULES, **PRICE_CHANGE_WARNING_RULES})
@dp.expect_all_or_fail(PRICE_CHANGE_CRITICAL_RULES)
def price_changes_evaluated():
    return (
        spark.readStream.table("events_price_changed")
        .dropDuplicates(["event_id"])
        .withColumn("failed_rules",failed_rules(PRICE_CHANGE_QUARANTINE_RULES))
    )

@dp.temporary_view(name="listings_evaluated")
@dp.expect_all({**QUARANTINE_RULES, **WARNING_RULES})
@dp.expect_all_or_fail(CRITICAL_RULES)
def price_changes_evaluated():
    return (
        spark.readStream.table("csv_houses_madrid")
        .dropDuplicates(["event_id"])
        .withColumn("failed_rules",failed_rules(QUARANTINE_RULES))
    )


@dp.table(name="re_dev.silver.price_changes")
def price_changes():
    """Valid price change events: they pass every quarantine rule."""
    return (
        spark.readStream.table("price_changes_evaluated")
        .filter(F.size("failed_rules") == 0)
        .drop("failed_rules")
    )
 
 
@dp.table(name="re_dev.silver.price_changes_quarantine")
def price_changes_quarantine():
    """Rejected price change events, with the rules each one breaks."""
    return (
        spark.readStream.table("price_changes_evaluated")
        .filter(F.size("failed_rules") > 0)
    )