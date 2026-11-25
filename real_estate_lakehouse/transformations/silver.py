import pyspark.pipelines as dp
from pyspark.sql import functions as F
from utilities.quality import failed_rules
from utilities.rules.price_changes_rules import PRICE_CHANGE_CRITICAL_RULES, PRICE_CHANGE_QUARANTINE_RULES, PRICE_CHANGE_WARNING_RULES
from utilities.rules.listings_rules import LISTING_CRITICAL_RULES, LISTING_QUARANTINE_RULES, LISTING_WARNING_RULES

from utilities.listings import cast_listing_columns, null_unknown_prices


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
@dp.expect_all({**LISTING_QUARANTINE_RULES, **LISTING_WARNING_RULES})
@dp.expect_all_or_fail(LISTING_CRITICAL_RULES)
def listings_evaluated():
    return (
        spark.readStream.table("csv_houses_madrid")
        .transform(cast_listing_columns)
        .transform(null_unknown_prices)
        .withColumn("failed_rules",failed_rules(LISTING_QUARANTINE_RULES))
    )

# Table Listings
@dp.table(name="silver.listings")
def listings():
    return (
        spark.readStream.table("listings_evaluated")
        .filter(F.size("failed_rules") == 0)
        .drop("failed_rules")
    )
 
 
@dp.table(name="silver.listings_quarantine")
def listings_quarantine():
    return (
        spark.readStream.table("listings_evaluated")
        .filter(F.size("failed_rules") > 0)
    )

# Table Price Changes
@dp.table(name="silver.price_changes")
def price_changes():
    """Valid price change events: they pass every quarantine rule."""
    return (
        spark.readStream.table("price_changes_evaluated")
        .filter(F.size("failed_rules") == 0)
        .drop("failed_rules")
    )
 
 
@dp.table(name="silver.price_changes_quarantine")
def price_changes_quarantine():
    """Rejected price change events, with the rules each one breaks."""
    return (
        spark.readStream.table("price_changes_evaluated")
        .filter(F.size("failed_rules") > 0)
    )