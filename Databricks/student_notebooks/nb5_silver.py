# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 5b: Silver Layer, Clean and Conform
# MAGIC 
# MAGIC **Course**: Distributed Systems for Data Science  
# MAGIC **Week**: 8, Data Pipelines & the Medallion Architecture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC 
# MAGIC This is the **Silver** layer. Silver reads from Bronze and transforms messy raw data
# MAGIC into a **single source of truth**: properly typed, deduplicated, and standardized.
# MAGIC 
# MAGIC After this notebook runs, every downstream query, dashboard, and Gold table can trust
# MAGIC the data without worrying about dollar signs in numbers, trailing spaces in tickers,
# MAGIC or duplicate rows from the custodian.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC Run `nb5_bronze` first. The Bronze tables must exist:
# MAGIC - `workspace.pipeline.bronze_holdings`
# MAGIC - `workspace.pipeline.bronze_prices`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read from Bronze
# MAGIC
# MAGIC Silver always reads from Bronze, never from the raw file. This decoupling means
# MAGIC if we need to change our cleaning logic, we re-run Silver without re-uploading
# MAGIC the file or re-running Bronze.

# COMMAND ----------

# ── READ BRONZE TABLES ───────────────────────────────────────────
bronze_holdings = spark.table("workspace.pipeline.bronze_holdings")
bronze_prices = spark.table("workspace.pipeline.bronze_prices")

h_count = bronze_holdings.count()
p_count = bronze_prices.count()
print(f"Bronze holdings: {h_count} rows")
print(f"Bronze prices:   {p_count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Clean Holdings
# MAGIC
# MAGIC The Holdings tab has several data quality issues from the custodian:
# MAGIC - Blank rows scattered through the data
# MAGIC - Tickers with trailing spaces (e.g., `"TSLA  "`)
# MAGIC - Tickers with exchange prefixes (e.g., `"NYSE:MSFT"`)
# MAGIC - Cost values with embedded dollar signs (e.g., `"$142.50"`)
# MAGIC - Duplicate rows from the custodian double-reporting positions
# MAGIC
# MAGIC We fix all of these and write a clean, properly-typed Delta table.
# MAGIC
# MAGIC Note: Bronze already renamed columns with spaces to underscores
# MAGIC (e.g., `Cost Basis` became `Cost_Basis`) because Delta does not allow
# MAGIC spaces in column names.

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, regexp_replace, current_timestamp, when
)
from pyspark.sql.types import DoubleType, IntegerType

# ── CLEAN HOLDINGS ───────────────────────────────────────────────

# Step 2a: Drop blank rows (Ticker is "nan" as a string from Bronze)
holdings = bronze_holdings.filter(
    (col("Ticker") != "nan") & (col("Ticker").isNotNull())
)
after_blanks = holdings.count()
print(f"After removing blank rows: {after_blanks} (removed {h_count - after_blanks})")

# Step 2b: Normalize tickers
#   - Strip whitespace:       "TSLA  " → "TSLA"
#   - Remove exchange prefix: "NYSE:MSFT" → "MSFT"
holdings = (holdings
    .withColumn("Ticker", trim(col("Ticker")))
    .withColumn("Ticker", regexp_replace(col("Ticker"), r"^[A-Z]+:", ""))
)

# Step 2c: Clean Cost_Basis
#   - Remove dollar signs: "$142.50" -> "142.50"
#   - Remove commas: "1,234.56" -> "1234.56"
#   - Cast to double
holdings = (holdings
    .withColumn("Cost_Basis", regexp_replace(col("Cost_Basis"), r"[\$,]", ""))
    .withColumn("Cost_Basis", col("Cost_Basis").cast(DoubleType()))
)

# Step 2d: Cast Shares_Held to integer
#   - Strip any non-numeric characters (Excel sometimes prefixes with apostrophe)
holdings = (holdings
    .withColumn("Shares_Held", regexp_replace(col("Shares_Held"), r"[^0-9]", ""))
    .withColumn("Shares_Held", col("Shares_Held").cast(IntegerType()))
)

# Step 2e: Deduplicate (keep first occurrence per ticker)
before_dedup = holdings.count()
holdings = holdings.dropDuplicates(["Ticker"])
after_dedup = holdings.count()
print(f"After deduplication: {after_dedup} (removed {before_dedup - after_dedup} duplicates)")

# Step 2f: Drop Bronze metadata, add Silver metadata
silver_holdings = (holdings
    .drop("_source_file", "_source_sheet", "_ingested_at")
    .withColumn("_processed_at", current_timestamp())
)

# COMMAND ----------

# ── WRITE SILVER HOLDINGS ────────────────────────────────────────
silver_holdings.write.mode("overwrite").saveAsTable("workspace.pipeline.silver_holdings")
print("Created: workspace.pipeline.silver_holdings")

# COMMAND ----------

print("SILVER HOLDINGS SCHEMA (properly typed now):")
spark.table("workspace.pipeline.silver_holdings").printSchema()

# COMMAND ----------

display(spark.table("workspace.pipeline.silver_holdings").limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Clean Prices
# MAGIC
# MAGIC The Prices tab has its own set of issues:
# MAGIC - Tickers with trailing spaces
# MAGIC - Dates in mixed formats (`"2024-03-15"` vs `"03/15/2024"`)
# MAGIC - Close prices with dollar signs or commas
# MAGIC - Missing prices marked as `"N/A"` instead of null
# MAGIC - Change values stored as `"-"` instead of a number
# MAGIC - Duplicate rows from custodia double-reporting

# COMMAND ----------

from pyspark.sql.functions import to_date, coalesce

# ── CLEAN PRICES ─────────────────────────────────────────────────

# Step 3a: Normalize symbols (same as tickers: strip whitespace)
prices = bronze_prices.withColumn("Symbol", trim(col("Symbol")))

# Step 3b: Normalize dates (handle both formats)
#   Use try_to_date instead of to_date because ANSI-strict mode (V5/Photon)
#   throws an exception on parse failure. try_to_date returns null instead.
from pyspark.sql.functions import expr
prices = prices.withColumn("Date",
    coalesce(
        expr("try_to_date(Date, 'yyyy-MM-dd')"),
        expr("try_to_date(Date, 'MM/dd/yyyy')")
    )
)

# Step 3c: Clean close prices
#   - Remove dollar signs and commas
#   - Replace "N/A" with null
#   - Cast to double
prices = (prices
    .withColumn("Close", regexp_replace(col("Close"), r"[\$,]", ""))
    .withColumn("Close",
        when(col("Close") == "N/A", None)
        .otherwise(col("Close"))
        .cast(DoubleType())
    )
)

# Step 3d: Clean Change% (replace "-" with null, cast to double)
prices = (prices
    .withColumn("Change%",
        when(col("Change%") == "-", None)
        .otherwise(col("Change%"))
        .cast(DoubleType())
    )
)

# Step 3e: Cast Volume to integer (strip non-numeric chars first)
prices = (prices
    .withColumn("Volume", regexp_replace(col("Volume"), r"[^0-9]", ""))
    .withColumn("Volume", col("Volume").cast(IntegerType()))
)

# Step 3f: Filter out rows with null prices (were "N/A" in the raw data)
before_filter = prices.count()
prices = prices.filter(col("Close").isNotNull())
after_filter = prices.count()
print(f"After filtering null prices: {after_filter} (removed {before_filter - after_filter})")

# Step 3g: Deduplicate
before_dedup = prices.count()
prices = prices.dropDuplicates(["Symbol", "Date"])
after_dedup = prices.count()
print(f"After deduplication: {after_dedup} (removed {before_dedup - after_dedup} duplicates)")

# Step 3h: Drop Bronze metadata, add Silver metadata
silver_prices = (prices
    .drop("_source_file", "_source_sheet", "_ingested_at")
    .withColumn("_processed_at", current_timestamp())
)

# COMMAND ----------

# ── WRITE SILVER PRICES ──────────────────────────────────────────
silver_prices.write.mode("overwrite").saveAsTable("workspace.pipeline.silver_prices")
print("Created: workspace.pipeline.silver_prices")

# COMMAND ----------

print("SILVER PRICES SCHEMA:")
spark.table("workspace.pipeline.silver_prices").printSchema()

# COMMAND ----------

display(spark.table("workspace.pipeline.silver_prices").limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write Cleaned CSVs Back to Volume
# MAGIC
# MAGIC As part of the Silver process, we also write the cleaned data back to the Volume
# MAGIC as versioned CSV files. This is useful for two reasons:
# MAGIC
# MAGIC 1. **Auditability**: you can see exactly what the pipeline produced at each version
# MAGIC 2. **Interoperability**: downstream systems that cannot read Delta can read CSV
# MAGIC
# MAGIC We append a version identifier to each filename so that multiple runs do not
# MAGIC overwrite previous outputs.

# COMMAND ----------

from datetime import datetime

# ── WRITE VERSIONED CSVs TO VOLUME ───────────────────────────────
version = datetime.now().strftime("%Y%m%d_%H%M%S")
vol_base = "/Volumes/workspace/pipeline/raw_files"

holdings_csv_path = f"{vol_base}/holdings_v{version}.csv"
prices_csv_path = f"{vol_base}/prices_v{version}.csv"

# Write as single CSV files (coalesce to 1 partition)
(silver_holdings.drop("_processed_at")
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(holdings_csv_path))

(silver_prices.drop("_processed_at")
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(prices_csv_path))

print(f"Wrote versioned CSVs:")
print(f"  {holdings_csv_path}")
print(f"  {prices_csv_path}")
print()
print("Go to Catalog > workspace > pipeline > raw_files in the UI to see the files.")

# COMMAND ----------

# ── VERIFY: LIST ALL FILES IN VOLUME ─────────────────────────────
print("All files in Volume workspace.pipeline.raw_files:")
files = dbutils.fs.ls(vol_base)
for f in files:
    print(f"  {f.name:45s} {f.size:>10,} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: What Silver Did
# MAGIC
# MAGIC | Issue | Bronze (raw) | Silver (cleaned) |
# MAGIC |-------|-------------|-----------------|
# MAGIC | Ticker formatting | `"TSLA  "`, `"NYSE:MSFT"` | `"TSLA"`, `"MSFT"` |
# MAGIC | Cost_Basis | `"$142.50"`, `"1,234.56"` | `142.50`, `1234.56` (double) |
# MAGIC | Blank rows | Present | Removed |
# MAGIC | Duplicate rows | Present (3 holdings, 4 prices) | Removed |
# MAGIC | Date formats | `"2024-03-15"` and `"03/15/2024"` mixed | All `date` type |
# MAGIC | Missing prices | `"N/A"` as string | Filtered out |
# MAGIC | Change values | `"-"` as string | `null` (proper) |
# MAGIC | Column types | All strings | Proper: int, double, date |
# MAGIC
# MAGIC The Silver tables are now the **single source of truth**. Any query or Gold table
# MAGIC can read from Silver with confidence.
# MAGIC
# MAGIC **Next step**: Run `nb5_gold` to build the portfolio valuation.
