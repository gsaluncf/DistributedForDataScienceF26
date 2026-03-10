# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 5c: Gold Layer, Portfolio Valuation
# MAGIC 
# MAGIC **Course**: Distributed Systems for Data Science  
# MAGIC **Week**: 8, Data Pipelines & the Medallion Architecture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC 
# MAGIC This is the **Gold** layer. Gold reads from Silver and builds **business-ready tables**
# MAGIC designed for specific use cases. No more raw data, no more cleaning. Just aggregated,
# MAGIC denormalized tables that answer the questions a portfolio manager asks every morning:
# MAGIC
# MAGIC - What do I own and what is it worth?
# MAGIC - How is my exposure distributed across sectors?
# MAGIC - Which positions gained or lost the most today?
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC Run `nb5_bronze` and `nb5_silver` first. The Silver tables must exist:
# MAGIC - `workspace.pipeline.silver_holdings`
# MAGIC - `workspace.pipeline.silver_prices`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build the Portfolio Valuation Table
# MAGIC
# MAGIC Join Holdings (what we own) with Prices (what it's worth today) to compute
# MAGIC the market value of each position and its weight in the portfolio.
# MAGIC
# MAGIC This is a classic Gold table: it combines two Silver sources into a single
# MAGIC denormalized view optimized for fast querying.

# COMMAND ----------

from pyspark.sql.functions import col, round as spark_round, sum as spark_sum, lit

# ── JOIN HOLDINGS + PRICES ───────────────────────────────────────
holdings = spark.table("workspace.pipeline.silver_holdings")
prices = spark.table("workspace.pipeline.silver_prices")

portfolio = (holdings
    .join(prices, holdings["Ticker"] == prices["Symbol"], "inner")
    .select(
        holdings["Ticker"],
        holdings["Company_Name"],
        holdings["Sector"],
        holdings["Shares_Held"],
        holdings["Cost_Basis"],
        prices["Close"].alias("Current_Price"),
        prices["Change%"].alias("Daily_Change_Pct"),
        prices["Date"].alias("Price_Date")
    )
)

# Compute derived columns
portfolio = (portfolio
    .withColumn("Market_Value",
        spark_round(col("Shares_Held") * col("Current_Price"), 2))
    .withColumn("Cost_Value",
        spark_round(col("Shares_Held") * col("Cost_Basis"), 2))
    .withColumn("Unrealized_PnL",
        spark_round(col("Market_Value") - col("Cost_Value"), 2))
)

# Compute portfolio-level weight (% of total market value)
total_value = portfolio.agg(spark_sum("Market_Value")).collect()[0][0]
portfolio = portfolio.withColumn("Weight_Pct",
    spark_round(col("Market_Value") / lit(total_value) * 100, 2))

portfolio_count = portfolio.count()
print(f"Portfolio valuation: {portfolio_count} positions")
print(f"Total portfolio value: ${total_value:,.2f}")

# COMMAND ----------

# ── WRITE GOLD: PORTFOLIO VALUATION ──────────────────────────────
(portfolio
    .orderBy(col("Market_Value").desc())
    .write.mode("overwrite")
    .saveAsTable("workspace.pipeline.gold_portfolio"))

print("Created: workspace.pipeline.gold_portfolio")

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Portfolio

# COMMAND ----------

display(spark.table("workspace.pipeline.gold_portfolio"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build the Sector Allocation Table
# MAGIC
# MAGIC This Gold table aggregates the portfolio by sector. A portfolio manager uses this
# MAGIC to check exposure: "Am I too concentrated in Technology?" or "Do I have enough
# MAGIC diversification across Energy and Healthcare?"

# COMMAND ----------

# ── SECTOR ALLOCATION ────────────────────────────────────────────
sector_alloc = (spark.table("workspace.pipeline.gold_portfolio")
    .groupBy("Sector")
    .agg(
        spark_sum("Market_Value").alias("Sector_Value"),
        spark_sum("Shares_Held").alias("Total_Shares"),
        spark_round(spark_sum("Unrealized_PnL"), 2).alias("Sector_PnL"),
    )
    .withColumn("Allocation_Pct",
        spark_round(col("Sector_Value") / lit(total_value) * 100, 2))
    .orderBy(col("Sector_Value").desc())
)

sector_alloc.write.mode("overwrite").saveAsTable("workspace.pipeline.gold_sector_allocation")
print("Created: workspace.pipeline.gold_sector_allocation")

# COMMAND ----------

display(spark.table("workspace.pipeline.gold_sector_allocation"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Pipeline Summary
# MAGIC
# MAGIC Let's look at the entire pipeline in one query: how many rows at each layer?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Bronze: Holdings' AS layer, COUNT(*) AS rows FROM workspace.pipeline.bronze_holdings
# MAGIC UNION ALL
# MAGIC SELECT 'Bronze: Prices', COUNT(*) FROM workspace.pipeline.bronze_prices
# MAGIC UNION ALL
# MAGIC SELECT 'Silver: Holdings', COUNT(*) FROM workspace.pipeline.silver_holdings
# MAGIC UNION ALL
# MAGIC SELECT 'Silver: Prices', COUNT(*) FROM workspace.pipeline.silver_prices
# MAGIC UNION ALL
# MAGIC SELECT 'Gold: Portfolio', COUNT(*) FROM workspace.pipeline.gold_portfolio
# MAGIC UNION ALL
# MAGIC SELECT 'Gold: Sectors', COUNT(*) FROM workspace.pipeline.gold_sector_allocation
# MAGIC ORDER BY
# MAGIC   CASE layer
# MAGIC     WHEN 'Bronze: Holdings' THEN 1
# MAGIC     WHEN 'Bronze: Prices' THEN 2
# MAGIC     WHEN 'Silver: Holdings' THEN 3
# MAGIC     WHEN 'Silver: Prices' THEN 4
# MAGIC     WHEN 'Gold: Portfolio' THEN 5
# MAGIC     WHEN 'Gold: Sectors' THEN 6
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Funnel
# MAGIC
# MAGIC Notice the pattern:
# MAGIC - **Bronze** has the most rows: raw data with blanks, duplicates, and bad records
# MAGIC - **Silver** has fewer rows: cleaned, deduplicated, properly typed
# MAGIC - **Gold** has the fewest rows: aggregated, purpose-built for consumption
# MAGIC
# MAGIC This funnel shape is the hallmark of the Medallion Architecture. Data gets smaller
# MAGIC and more refined at each layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Delta Versioning
# MAGIC
# MAGIC Every write to a Delta table creates a new version. You can see the full history
# MAGIC and even query previous versions with time travel.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.pipeline.gold_portfolio;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Cleanup
# MAGIC
# MAGIC Run this cell to drop all tables created by the pipeline.
# MAGIC
# MAGIC **Only run this after you have completed the Workflow exercise** (see the
# MAGIC [pipeline guide](pipeline.html) for instructions on setting up the Workflow).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop all pipeline tables
# MAGIC DROP TABLE IF EXISTS workspace.pipeline.bronze_holdings;
# MAGIC DROP TABLE IF EXISTS workspace.pipeline.bronze_prices;
# MAGIC DROP TABLE IF EXISTS workspace.pipeline.silver_holdings;
# MAGIC DROP TABLE IF EXISTS workspace.pipeline.silver_prices;
# MAGIC DROP TABLE IF EXISTS workspace.pipeline.gold_portfolio;
# MAGIC DROP TABLE IF EXISTS workspace.pipeline.gold_sector_allocation;
# MAGIC 
# MAGIC -- Drop the Volume and schema
# MAGIC DROP VOLUME IF EXISTS workspace.pipeline.raw_files;
# MAGIC DROP SCHEMA IF EXISTS workspace.pipeline CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **End of the Medallion Pipeline.** You have built a complete Bronze, Silver, Gold
# MAGIC pipeline from a real-world custodian report. The same architecture is used in production
# MAGIC at investment firms, banks, and fintech companies processing millions of positions daily.
# MAGIC
# MAGIC Now go to the [pipeline guide](pipeline.html) and follow the instructions to wire
# MAGIC these three notebooks into a **Databricks Workflow** so you can see the entire
# MAGIC pipeline run as a visual DAG.
