# Databricks notebook source
# MAGIC %md
# MAGIC # Alteryx Converted Job 2
# MAGIC 
# MAGIC This notebook accepts client name, financial year, and accounting period parameters.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Input Parameters

# COMMAND ----------

# Get parameters from widgets
client_name = dbutils.widgets.get("client_name")
financial_year = dbutils.widgets.get("financial_year")
accounting_period = dbutils.widgets.get("accounting_period")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Print Parameters

# COMMAND ----------

print("=" * 50)
print("Job Parameters:")
print("=" * 50)
print(f"Client Name: {client_name}")
print(f"Financial Year: {financial_year}")
print(f"Accounting Period: {accounting_period}")
print("=" * 50)

