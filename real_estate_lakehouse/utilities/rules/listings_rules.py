"""
Data quality rules for the silver listings, evaluated after typing and sentinel cleaning.
"""

LISTING_CRITICAL_RULES = {
    "has_listing_id": "listing_id IS NOT NULL",
}

LISTING_QUARANTINE_RULES = {
    "positive_sq_mt_built": "sq_mt_built > 0",
    "valid_buy_price": "buy_price IS NULL OR buy_price > 0",
    "has_neighborhood_id": "neighborhood_id IS NOT NULL",
}

LISTING_WARNING_RULES = {
    "no_rescued_data": "_rescued_data IS NULL",
}