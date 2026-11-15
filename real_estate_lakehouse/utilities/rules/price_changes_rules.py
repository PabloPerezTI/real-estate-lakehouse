PRICE_CHANGE_CRITICAL_RULES = {
    "has_event_id": "event_id IS NOT NULL",
}

PRICE_CHANGE_QUARANTINE_RULES = {
    "has_listing_id": "listing_id IS NOT NULL",
    "positive_new_price": "new_price > 0",
}

PRICE_CHANGE_WARNING_RULES = {
    "no_rescued_data": "_rescued_data IS NULL",
}