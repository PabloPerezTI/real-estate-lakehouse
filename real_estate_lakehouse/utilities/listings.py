"""
Transformations for the listings of the initial load: column contract,
typing, renaming and sentinel cleaning. Pure functions (DataFrame in,
DataFrame out), so they can be unit tested outside the pipeline.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

LISTING_COLUMN_TYPES = {
    "listing_id": "INT",
    "source_listing_id": "INT",
    "title": "STRING",
    "subtitle": "STRING",
    "sq_mt_built": "DOUBLE",
    "sq_mt_useful": "DOUBLE",
    "sq_mt_allotment": "DOUBLE",
    "n_rooms": "INT",
    "n_bathrooms": "INT",
    "n_floors": "INT",
    "latitude": "DOUBLE",
    "longitude": "DOUBLE",
    "raw_address": "STRING",
    "is_exact_address_hidden": "BOOLEAN",
    "street_name": "STRING",
    "street_number": "STRING",
    "portal": "STRING",
    "floor": "STRING",
    "is_floor_under": "BOOLEAN",
    "door": "STRING",
    "neighborhood_id": "STRING",
    "operation": "STRING",
    "rent_price": "INT",
    "rent_price_by_area": "INT",
    "is_rent_price_known": "BOOLEAN",
    "buy_price": "INT",
    "buy_price_by_area": "INT",
    "is_buy_price_known": "BOOLEAN",
    "house_type_id": "STRING",
    "is_renewal_needed": "BOOLEAN",
    "is_new_development": "BOOLEAN",
    "built_year": "INT",
    "has_central_heating": "BOOLEAN",
    "has_individual_heating": "BOOLEAN",
    "are_pets_allowed": "BOOLEAN",
    "has_ac": "BOOLEAN",
    "has_fitted_wardrobes": "BOOLEAN",
    "has_lift": "BOOLEAN",
    "is_exterior": "BOOLEAN",
    "has_garden": "BOOLEAN",
    "has_pool": "BOOLEAN",
    "has_terrace": "BOOLEAN",
    "has_balcony": "BOOLEAN",
    "has_storage_room": "BOOLEAN",
    "is_furnished": "BOOLEAN",
    "is_kitchen_equipped": "BOOLEAN",
    "is_accessible": "BOOLEAN",
    "has_green_zones": "BOOLEAN",
    "energy_certificate": "STRING",
    "has_parking": "BOOLEAN",
    "has_private_parking": "BOOLEAN",
    "has_public_parking": "BOOLEAN",
    "is_parking_included_in_price": "BOOLEAN",
    "parking_price": "INT",
    "is_orientation_north": "BOOLEAN",
    "is_orientation_west": "BOOLEAN",
    "is_orientation_south": "BOOLEAN",
    "is_orientation_east": "BOOLEAN",
}

METADATA_COLUMNS = ["_rescued_data", "_source_file", "_ingested_at"]
IGNORED_SOURCE_COLUMNS = {}

PRICE_FLAGS = {
    "buy_price": "is_buy_price_known",
    "buy_price_by_area": "is_buy_price_known",
    "rent_price": "is_rent_price_known",
    "rent_price_by_area": "is_rent_price_known",
}


def typed_column(name: str, sql_type: str) -> Column:
    """
    Cast a raw text column to its type with try_cast, so a value that does not
    fit becomes NULL instead of failing the pipeline. Integers go through DOUBLE
    first so that values written as "149.0" are still read as 149.
    """
    if sql_type == "INT":
        return F.expr(f"try_cast(try_cast(`{name}` AS DOUBLE) AS INT)").alias(name)
    return F.expr(f"try_cast(`{name}` AS {sql_type})").alias(name)


def cast_listing_columns(df: DataFrame) -> DataFrame:
    """
    Rename id to listing_id, give every column its type and keep the ingestion
    metadata. Fails at pipeline start if bronze has a column the contract does
    not know, so a schema change in the source never goes unnoticed.
    """
    df = df.withColumnRenamed("id", "listing_id")
    df = df.withColumnRenamed("id_pisos", "source_listing_id")
    known = set(LISTING_COLUMN_TYPES) | set(METADATA_COLUMNS) # | IGNORED_SOURCE_COLUMNS
    unexpected = set(df.columns) - known
    if unexpected:
        raise ValueError(f"Unexpected columns in bronze listings: {sorted(unexpected)}")

    typed = [typed_column(name, sql_type) for name, sql_type in LISTING_COLUMN_TYPES.items()]
    metadata = [F.col(name) for name in METADATA_COLUMNS if name in df.columns]
    return df.select(*typed, *metadata)


def null_unknown_prices(df: DataFrame) -> DataFrame:
    """
    Apply the sentinel rule: a price is kept only when its flag is TRUE.
    A FALSE or NULL flag turns the price into NULL (unknown).
    """
    for price, flag in PRICE_FLAGS.items():
        df = df.withColumn(price, F.when(F.col(flag), F.col(price)))
    return df