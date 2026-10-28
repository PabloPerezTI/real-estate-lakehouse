# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC SELECT * FROM 
# MAGIC re_dev.bronze.events_price_changed
# MAGIC limit 30;