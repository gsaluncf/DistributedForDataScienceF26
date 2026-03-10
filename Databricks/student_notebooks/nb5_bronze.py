# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 5a: Bronze Layer, Raw Ingestion
# MAGIC 
# MAGIC **Course**: Distributed Systems for Data Science  
# MAGIC **Week**: 8, Data Pipelines & the Medallion Architecture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC 
# MAGIC This is the **Bronze** layer of the Medallion Architecture. Bronze has one job:
# MAGIC **capture exactly what arrived and persist it to durable storage.** No cleaning,
# MAGIC no type casting, no assumptions. Just save the raw data as a Delta table with
# MAGIC ingestion metadata.
# MAGIC
# MAGIC ### Before Running This Notebook
# MAGIC
# MAGIC You should have already completed the **manual upload** step:
# MAGIC
# MAGIC 1. Created a schema: `workspace.pipeline`
# MAGIC 2. Created a Volume: `workspace.pipeline.raw_files`
# MAGIC 3. Uploaded `custodian_report.xlsx` to that Volume via the Databricks UI
# MAGIC
# MAGIC If you have not done this yet, follow the instructions in the
# MAGIC [pipeline guide](pipeline.html) before continuing.
# MAGIC
# MAGIC > **Reading**: [Data Pipelines & the Medallion Architecture](pipeline.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Install openpyxl
# MAGIC
# MAGIC The `openpyxl` library is what pandas uses to read `.xlsx` files.
# MAGIC It is not pre-installed on Databricks serverless compute.
# MAGIC **This must be the very first code cell**, because `%pip install`
# MAGIC automatically restarts the Python environment, which clears all variables.

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Verify the Upload
# MAGIC
# MAGIC Before we do anything, let's confirm the file is sitting in the Volume.
# MAGIC This is the same as checking that a file landed in an S3 bucket before
# MAGIC your Lambda function processes it.

# COMMAND ----------

# ── VERIFY THE UPLOADED FILE ─────────────────────────────────────
files = dbutils.fs.ls("/Volumes/workspace/pipeline/raw_files/")
print("Files in Volume workspace.pipeline.raw_files:")
for f in files:
    print(f"  {f.name}  ({f.size:,} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Read the Raw Excel
# MAGIC
# MAGIC The custodian sent an Excel file with two tabs: **Holdings** and **Prices**.
# MAGIC We need to read both. On Databricks, we use `pandas` to read Excel files
# MAGIC (Spark does not natively read `.xlsx`), then convert to Spark DataFrames.
# MAGIC
# MAGIC Notice that the Holdings sheet has a **title block** in rows 1-4 before the
# MAGIC actual data headers. This is common in custodian reports, and it is exactly the
# MAGIC kind of messiness that Bronze captures without trying to fix.

# COMMAND ----------

import pandas as pd

# ── READ HOLDINGS TAB ────────────────────────────────────────────
# The custodian put a title block in rows 1-4, so the real headers are on row 5
# In pandas, header=4 means "use row index 4 (the 5th row) as column names"
vol_path = "/Volumes/workspace/pipeline/raw_files/custodian_report.xlsx"

holdings_pdf = pd.read_excel(vol_path, sheet_name="Holdings", header=4)
print(f"Holdings tab: {len(holdings_pdf)} rows x {len(holdings_pdf.columns)} columns")
print(f"Columns: {list(holdings_pdf.columns)}")
print()

# ── READ PRICES TAB ──────────────────────────────────────────────
prices_pdf = pd.read_excel(vol_path, sheet_name="Prices")
print(f"Prices tab: {len(prices_pdf)} rows x {len(prices_pdf.columns)} columns")
print(f"Columns: {list(prices_pdf.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Convert to Spark DataFrames
# MAGIC
# MAGIC We read the data with pandas because Spark cannot read `.xlsx` natively.
# MAGIC Now we convert to Spark DataFrames so we can write them as Delta tables.
# MAGIC
# MAGIC Everything stays as strings at this stage. Bronze does not interpret the data,
# MAGIC it just persists what arrived.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

print("Converting pandas DataFrames to Spark DataFrames...")
print("(The first Spark operation may take 1-3 minutes on serverless while compute provisions.)")
print()

# Cast everything to strings: Bronze makes no assumptions about types
for c in holdings_pdf.columns:
    holdings_pdf[c] = holdings_pdf[c].astype(str)
for c in prices_pdf.columns:
    prices_pdf[c] = prices_pdf[c].astype(str)
print("  Columns cast to strings.")

# Delta does not allow spaces in column names, so replace them with underscores
holdings_pdf.columns = [c.replace(" ", "_") for c in holdings_pdf.columns]
prices_pdf.columns = [c.replace(" ", "_") for c in prices_pdf.columns]
print(f"  Column names sanitized: {list(holdings_pdf.columns)}")

holdings_df = spark.createDataFrame(holdings_pdf)
print(f"  Holdings Spark DataFrame created: {len(holdings_pdf)} rows")

prices_df = spark.createDataFrame(prices_pdf)
print(f"  Prices Spark DataFrame created: {len(prices_pdf)} rows")

# Add ingestion metadata
holdings_bronze = (holdings_df
    .withColumn("_source_file", lit("custodian_report.xlsx"))
    .withColumn("_source_sheet", lit("Holdings"))
    .withColumn("_ingested_at", current_timestamp()))

prices_bronze = (prices_df
    .withColumn("_source_file", lit("custodian_report.xlsx"))
    .withColumn("_source_sheet", lit("Prices"))
    .withColumn("_ingested_at", current_timestamp()))

print(f"Holdings DataFrame: {holdings_bronze.count()} rows")
print(f"Prices DataFrame:   {prices_bronze.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write to Delta (Bronze Tables)
# MAGIC
# MAGIC Now we persist both DataFrames as Delta tables in the `pipeline` schema.
# MAGIC These are the Bronze tables: raw data with metadata, ready for Silver to clean.

# COMMAND ----------

# ── WRITE BRONZE DELTA TABLES ────────────────────────────────────
holdings_bronze.write.mode("overwrite").saveAsTable("workspace.pipeline.bronze_holdings")
prices_bronze.write.mode("overwrite").saveAsTable("workspace.pipeline.bronze_prices")

print("Bronze tables created:")
print("  workspace.pipeline.bronze_holdings")
print("  workspace.pipeline.bronze_prices")

# COMMAND ----------

# ── INSPECT BRONZE: HOLDINGS ─────────────────────────────────────
print("BRONZE HOLDINGS SCHEMA (everything is strings, raw data):")
spark.table("workspace.pipeline.bronze_holdings").printSchema()

# COMMAND ----------

display(spark.table("workspace.pipeline.bronze_holdings").limit(15))

# COMMAND ----------

# ── INSPECT BRONZE: PRICES ───────────────────────────────────────
print("BRONZE PRICES SCHEMA:")
spark.table("workspace.pipeline.bronze_prices").printSchema()

# COMMAND ----------

display(spark.table("workspace.pipeline.bronze_prices").limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What Just Happened
# MAGIC
# MAGIC We read the raw Excel file from the Volume and wrote both tabs as Delta tables
# MAGIC without changing anything. The Bronze tables contain:
# MAGIC
# MAGIC - All original data, including blank rows, messy tickers, and dollar signs in numbers
# MAGIC - Ingestion metadata: which file, which sheet, when it was loaded
# MAGIC - Everything typed as strings so that no data is lost during type casting
# MAGIC
# MAGIC The Bronze layer is now a **durable, versioned checkpoint**. If the Silver logic
# MAGIC has a bug, we can always come back to Bronze and reprocess without re-uploading
# MAGIC the file. This is the same principle as keeping raw messages in an SQS dead letter
# MAGIC queue so you can replay them later.
# MAGIC
# MAGIC **Next step**: Run `nb5_silver` to clean this data.
