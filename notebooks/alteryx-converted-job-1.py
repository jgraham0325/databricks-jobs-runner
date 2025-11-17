# Databricks notebook source
# MAGIC %md
# MAGIC # Alteryx Converted Job 1
# MAGIC 
# MAGIC This notebook accepts date range and client ID parameters.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Input Parameters

# COMMAND ----------

# Get parameters from widgets
startDate = dbutils.widgets.get("startDate")
endDate = dbutils.widgets.get("endDate")
clientID = dbutils.widgets.get("clientID")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Print Parameters

# COMMAND ----------

print("=" * 50)
print("Job Parameters:")
print("=" * 50)
print(f"Start Date: {startDate}")
print(f"End Date: {endDate}")
print(f"Client ID: {clientID}")
print("=" * 50)

