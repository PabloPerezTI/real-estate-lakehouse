"""
Data quality helpers shared by the silver tables.
"""

from pyspark.sql import Column, functions as F

def failed_rules(rules: dict) -> Column:
    checks = []
    for name, rule in rules.items():
        checks.append(F.when(~F.coalesce(F.expr(rule),F.lit(False)), F.lit(name)))
    return F.filter(F.array(*checks), lambda rule: rule.isNotNull())