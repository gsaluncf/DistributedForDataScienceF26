# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 6: Declarative Pipeline (Lakeflow DLT)
# MAGIC
# MAGIC **Course**: Distributed Systems for Data Science  
# MAGIC **Week**: 8, Data Pipelines & the Medallion Architecture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC
# MAGIC This notebook builds a **Medallion pipeline** using a completely different
# MAGIC paradigm from notebooks 5a-5c: **declarative table definitions** instead
# MAGIC of imperative notebook chaining.
# MAGIC
# MAGIC Instead of writing "read this, clean that, write here," you declare
# MAGIC "this table should contain..." and let Databricks figure out the
# MAGIC execution order, error handling, and optimization.
# MAGIC
# MAGIC The data is embedded directly in this notebook so the pipeline is
# MAGIC **fully self-contained**, no file uploads or prior notebooks required.
# MAGIC
# MAGIC ### How to Run This Notebook
# MAGIC
# MAGIC **This notebook cannot be run cell-by-cell.** It must be run as a
# MAGIC **Pipeline** (not a regular notebook or job):
# MAGIC
# MAGIC 1. Go to **Jobs & Pipelines** in the sidebar
# MAGIC 2. Switch to the **Pipelines** tab
# MAGIC 3. Click **Create pipeline**
# MAGIC 4. Add this notebook as the source code
# MAGIC 5. Set destination catalog to `workspace`, schema to `dlt_demo`
# MAGIC 6. Click **Create**, then **Start**
# MAGIC
# MAGIC See the [Declarative Pipelines guide](declarative.html) for detailed instructions.
# MAGIC
# MAGIC > **Reading**: [Declarative Pipelines: The Modern Approach](declarative.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer: Raw Data
# MAGIC
# MAGIC In a real pipeline, Bronze would ingest from a file or stream.
# MAGIC Here we simulate the custodian's messy data inline to keep the
# MAGIC pipeline self-contained. Notice the intentional data quality issues:
# MAGIC tickers with exchange prefixes, dollar signs in costs, mixed date
# MAGIC formats, N/A values, and duplicate rows.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, trim, regexp_replace, current_timestamp, lit, when, coalesce
)
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructType, StructField
)

# ── BRONZE: RAW HOLDINGS (simulated custodian data) ──────────────
_holdings_data = [
    ("AAPL",      "Apple Inc",           "500",  "$142.50", "Technology",  "MAIN"),
    ("NYSE:MSFT", "Microsoft Corp",      "300",  "$71.30",  "Technology",  "MAIN"),
    ("GOOGL",     "Alphabet Inc",        "150",  "297.77",  "Technology",  "MAIN"),
    ("AMZN",      "Amazon.com Inc",      "200",  "$330.09", "Consumer",    "MAIN"),
    ("META",      "Meta Platforms",      "400",  "176.77",  "Technology",  "MAIN"),
    ("TSLA  ",    "Tesla Inc",           "350",  "$109.22", "Consumer",    "ALT"),
    ("JPM",       "JPMorgan Chase",      "600",  "76.36",   "Financials",  "MAIN"),
    ("NYSE:V",    "Visa Inc",            "250",  "$292.66", "Financials",  "MAIN"),
    ("JNJ",       "Johnson & Johnson",   "180",  "176.60",  "Healthcare",  "ALT"),
    ("UNH",       "UnitedHealth Group",  "120",  "527.18",  "Healthcare",  "ALT"),
    (None,        None,                  None,   None,      None,          None),  # blank row
    ("AAPL",      "Apple Inc",           "500",  "$142.50", "Technology",  "MAIN"),  # duplicate
]
_holdings_schema = ["Ticker","Company_Name","Shares_Held","Cost_Basis","Sector","Account"]

@dlt.table(
    comment="Raw holdings: simulated custodian data with intentional quality issues",
    table_properties={"quality": "bronze"}
)
def bronze_holdings():
    """Simulate raw holdings from a custodian report."""
    return spark.createDataFrame(_holdings_data, _holdings_schema)

# COMMAND ----------

# ── BRONZE: RAW PRICES (simulated market data) ──────────────────
_prices_data = [
    ("AAPL",  "2024-03-15",  "$164.30",  "52341000", "1.2"),
    ("MSFT",  "03/15/2024",  "$412.50",  "28500000", "0.8"),
    ("GOOGL", "2024-03-15",  "148.72",   "19800000", "-0.5"),
    ("AMZN",  "2024-03-15",  "$178.25",  "45200000", "2.1"),
    ("META",  "2024-03-15",  "$502.30",  "22100000", "1.9"),
    ("TSLA",  "03/15/2024",  "$175.10",  "98700000", "-3.2"),
    ("JPM",   "2024-03-15",  "$196.45",  "11200000", "0.3"),
    ("V",     "2024-03-15",  "$280.15",  "8900000",  "0.6"),
    ("JNJ",   "03/15/2024",  "$156.80",  "7200000",  "-0.1"),
    ("UNH",   "2024-03-15",  "$527.40",  "4100000",  "1.4"),
    ("AAPL",  "2024-03-15",  "$164.30",  "52341000", "1.2"),  # duplicate
    ("NVDA",  "2024-03-15",  "N/A",      "0",        "-"),    # N/A price
]
_prices_schema = ["Symbol","Date","Close","Volume","Change_Pct"]

@dlt.table(
    comment="Raw prices: simulated market data with mixed formats and bad records",
    table_properties={"quality": "bronze"}
)
def bronze_prices():
    """Simulate raw end-of-day prices from a market data vendor."""
    return spark.createDataFrame(_prices_data, _prices_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Clean and Conform
# MAGIC
# MAGIC Read from Bronze, clean the data, cast types, and deduplicate.
# MAGIC Notice the `@dlt.expect_or_drop` decorators: these are **data quality rules**.
# MAGIC Any row that violates the rule is automatically dropped, and the drop count
# MAGIC is tracked in the pipeline's data quality dashboard.

# COMMAND ----------

# ── SILVER: CLEANED HOLDINGS ─────────────────────────────────────
@dlt.table(
    comment="Cleaned holdings: normalized tickers, proper types, deduplicated",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_ticker", "Ticker IS NOT NULL")
@dlt.expect_or_drop("has_shares", "Shares_Held IS NOT NULL")
def silver_holdings():
    """
    Clean the raw holdings data:
    - Drop blank rows (null Ticker)
    - Strip whitespace and exchange prefixes from tickers
    - Remove dollar signs and commas from cost values
    - Cast types (shares to int, cost to double)
    - Deduplicate on ticker
    """
    return (dlt.read("bronze_holdings")
        # Normalize tickers: strip whitespace, remove "NYSE:" prefixes
        .withColumn("Ticker", trim(regexp_replace(col("Ticker"), r"^[A-Z]+:", "")))
        # Clean cost basis: "$142.50" -> 142.50
        .withColumn("Cost_Basis",
            regexp_replace(col("Cost_Basis"), r"[\$,]", "").cast(DoubleType()))
        # Cast shares to integer
        .withColumn("Shares_Held", regexp_replace(col("Shares_Held"), r"[^0-9]", "").cast(IntegerType()))
        # Deduplicate
        .dropDuplicates(["Ticker"])
    )

# COMMAND ----------

# ── SILVER: CLEANED PRICES ───────────────────────────────────────
@dlt.table(
    comment="Cleaned prices: parsed dates, proper types, deduplicated",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_symbol", "Symbol IS NOT NULL")
@dlt.expect_or_drop("valid_close", "Close IS NOT NULL")
def silver_prices():
    """
    Clean the raw prices data:
    - Strip whitespace from symbols
    - Normalize date formats to yyyy-MM-dd
    - Remove dollar signs and commas from close prices
    - Replace "N/A" with null, then drop nulls via expectation
    - Cast types
    - Deduplicate on (symbol, date)
    """
    from pyspark.sql.functions import to_date

    return (dlt.read("bronze_prices")
        # Normalize symbols
        .withColumn("Symbol", trim(col("Symbol")))
        # Normalize dates: convert MM/DD/YYYY -> YYYY-MM-DD using regex
        # then parse with a single format
        .withColumn("Date",
            when(col("Date").rlike(r"^\d{2}/\d{2}/\d{4}$"),
                regexp_replace(col("Date"), r"(\d{2})/(\d{2})/(\d{4})", "$3-$1-$2"))
            .otherwise(col("Date"))
        )
        .withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))
        # Clean close prices: "$164.30" -> 164.30, "N/A" -> null
        .withColumn("Close", regexp_replace(col("Close"), r"[\$,]", ""))
        .withColumn("Close",
            when(col("Close") == "N/A", None)
            .otherwise(col("Close"))
            .cast(DoubleType())
        )
        # Clean change%: "-" -> null
        .withColumn("Change_Pct",
            when(col("Change_Pct") == "-", None)
            .otherwise(col("Change_Pct"))
            .cast(DoubleType())
        )
        # Cast volume
        .withColumn("Volume", regexp_replace(col("Volume"), r"[^0-9]", "").cast(IntegerType()))
        # Deduplicate
        .dropDuplicates(["Symbol", "Date"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Portfolio Valuation
# MAGIC
# MAGIC Join Silver holdings with Silver prices to compute the portfolio valuation.
# MAGIC This function reads from TWO upstream tables, so Databricks knows it depends
# MAGIC on both `silver_holdings` and `silver_prices`.

# COMMAND ----------

# ── GOLD: PORTFOLIO VALUATION ────────────────────────────────────
@dlt.table(
    comment="Portfolio valuation: market value, P&L, and weight per position",
    table_properties={"quality": "gold"}
)
@dlt.expect("has_market_value", "Market_Value > 0")
def gold_portfolio():
    """
    Join cleaned holdings with cleaned prices to build the
    portfolio valuation table.
    """
    from pyspark.sql.functions import round as spark_round, sum as spark_sum

    holdings = dlt.read("silver_holdings")
    prices = dlt.read("silver_prices")

    portfolio = (holdings
        .join(prices, holdings["Ticker"] == prices["Symbol"], "inner")
        .select(
            holdings["Ticker"],
            holdings["Company_Name"],
            holdings["Sector"],
            holdings["Shares_Held"],
            holdings["Cost_Basis"],
            prices["Close"].alias("Current_Price"),
            prices["Change_Pct"].alias("Daily_Change_Pct"),
            prices["Date"].alias("Price_Date")
        )
        .withColumn("Market_Value",
            spark_round(col("Shares_Held") * col("Current_Price"), 2))
        .withColumn("Cost_Value",
            spark_round(col("Shares_Held") * col("Cost_Basis"), 2))
        .withColumn("Unrealized_PnL",
            spark_round(col("Market_Value") - col("Cost_Value"), 2))
    )

    # Compute portfolio weight
    total_value = portfolio.agg(spark_sum("Market_Value")).collect()[0][0]
    return portfolio.withColumn("Weight_Pct",
        spark_round(col("Market_Value") / lit(total_value) * 100, 2))

# COMMAND ----------

# ── GOLD: SECTOR ALLOCATION ──────────────────────────────────────
@dlt.table(
    comment="Sector allocation: total value and weight by sector",
    table_properties={"quality": "gold"}
)
def gold_sector_allocation():
    """
    Aggregate the portfolio by sector to show exposure distribution.
    This reads from gold_portfolio, adding another level to the DAG.
    """
    from pyspark.sql.functions import round as spark_round, sum as spark_sum

    portfolio = dlt.read("gold_portfolio")
    total = portfolio.agg(spark_sum("Market_Value")).collect()[0][0]

    return (portfolio
        .groupBy("Sector")
        .agg(
            spark_sum("Market_Value").alias("Sector_Value"),
            spark_sum("Shares_Held").alias("Total_Shares"),
            spark_round(spark_sum("Unrealized_PnL"), 2).alias("Sector_PnL"),
        )
        .withColumn("Allocation_Pct",
            spark_round(col("Sector_Value") / lit(total) * 100, 2))
        .orderBy(col("Sector_Value").desc())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to Notice
# MAGIC
# MAGIC When this pipeline runs, observe:
# MAGIC
# MAGIC 1. **The DAG was built for you.** You defined six functions with `dlt.read()` calls.
# MAGIC    Databricks resolved the order: Bronze first, then Silver, then Gold.
# MAGIC
# MAGIC 2. **Expectations tracked quality.** The pipeline UI shows how many rows passed
# MAGIC    each `@dlt.expect` rule and how many were dropped by `@dlt.expect_or_drop`.
# MAGIC    Check the Bronze vs Silver row counts: the blank row and the N/A price row
# MAGIC    were dropped automatically.
# MAGIC
# MAGIC 3. **One file, one pipeline.** Instead of three notebooks and a Workflow, everything
# MAGIC    is in this single file. If you need a new table, just add a new function.
# MAGIC
# MAGIC 4. **The sector allocation reads from Gold.** `gold_sector_allocation` depends on
# MAGIC    `gold_portfolio`, which depends on Silver, which depends on Bronze. The four-level
# MAGIC    DAG was resolved automatically.
# MAGIC
# MAGIC 5. **Compare to the imperative pipeline.** Notebooks 5a-5c required you to manually
# MAGIC    sequence Bronze, Silver, Gold in a Workflow. Here you just declared what each
# MAGIC    table should contain.
# MAGIC
# MAGIC This is the production standard for data pipelines on Databricks.
