# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 2: File Formats & Table Formats
# MAGIC 
# MAGIC **Course**: Distributed Systems for Data Science  
# MAGIC **Week**: 7 — Spark & the Lakehouse
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Why File Formats Matter in Distributed Systems
# MAGIC
# MAGIC In a distributed system, data lives on **many machines**. When Spark runs a query, 
# MAGIC each worker reads its portion of the data from storage. How that data is **organized on disk**
# MAGIC has a massive impact on:
# MAGIC
# MAGIC - **Query speed**: Can Spark skip irrelevant data?
# MAGIC - **Storage cost**: How much compression is possible?
# MAGIC - **Reliability**: What happens if a write fails halfway?
# MAGIC - **Schema safety**: Can bad data sneak in?
# MAGIC
# MAGIC We will explore five formats, from simplest to most powerful.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Row-Based vs Columnar Storage
# MAGIC
# MAGIC This is the most fundamental concept. Consider a table with 3 columns:
# MAGIC
# MAGIC ```
# MAGIC name     | score | city
# MAGIC ---------|-------|-----
# MAGIC Alice    |  95   | NYC
# MAGIC Bob      |  87   | LA
# MAGIC Carol    |  92   | SF
# MAGIC ```
# MAGIC
# MAGIC ### Row-Based (CSV, Avro, traditional databases)
# MAGIC Data is stored **one row at a time**:
# MAGIC ```
# MAGIC [Alice, 95, NYC] [Bob, 87, LA] [Carol, 92, SF]
# MAGIC ```
# MAGIC To answer "What is the average score?", you must read EVERY byte — 
# MAGIC including the names and cities you don't need.
# MAGIC
# MAGIC ### Columnar (Parquet, ORC, Delta Lake)
# MAGIC Data is stored **one column at a time**:
# MAGIC ```
# MAGIC names:  [Alice, Bob, Carol]
# MAGIC scores: [95, 87, 92]
# MAGIC cities: [NYC, LA, SF]
# MAGIC ```
# MAGIC To answer "What is the average score?", Spark reads ONLY the `scores` array.
# MAGIC With 100 columns, that is a **100x speedup** for single-column queries!
# MAGIC
# MAGIC **Key principle**: Analytics workloads (BI, dashboards, ML feature extraction) almost always
# MAGIC read a few columns from many rows. Columnar storage is built for this pattern.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Schema: None → Embedded → Enforced
# MAGIC
# MAGIC | Level | Format | What it means |
# MAGIC |-------|--------|--------------|
# MAGIC | **None** | CSV | No type information. Is "95" a string or an integer? The reader guesses. |
# MAGIC | **Embedded** | Parquet, Avro | Types are stored in the file header. Every reader knows exact types. |
# MAGIC | **Enforced** | Delta, Iceberg | Types are checked on WRITE. Bad data is rejected before it enters the table. |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The Format Evolution Story
# MAGIC
# MAGIC | Era | Format | Innovation |
# MAGIC |-----|--------|-----------|
# MAGIC | 1970s | CSV | "Humans can read it" |
# MAGIC | 2013 | Parquet (by Cloudera + Twitter) | Columnar + compression + embedded schema |
# MAGIC | 2011 | Avro (by Apache) | Row-based but with schema + schema evolution |
# MAGIC | 2019 | Delta Lake (by Databricks) | Parquet files + ACID transactions + time travel |
# MAGIC | 2018 | Apache Iceberg (by Netflix) | Same idea as Delta, but engine-agnostic (no vendor lock-in) |
# MAGIC
# MAGIC > 📖 [Parquet docs](https://parquet.apache.org/) | [Delta Lake docs](https://delta.io/) | [Iceberg docs](https://iceberg.apache.org/) | [Avro docs](https://avro.apache.org/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog — DNS for Data
# MAGIC
# MAGIC Before we write files, let's understand **where** data lives in Databricks.
# MAGIC
# MAGIC ### The Problem
# MAGIC In a distributed system, how do services **find** things?
# MAGIC - In your P2P project: nodes found peers via a **gossip protocol**
# MAGIC - In your AWS project: services found queues via **URLs**
# MAGIC - In a data lakehouse: how do hundreds of notebooks find thousands of tables?
# MAGIC
# MAGIC ### The Solution: Unity Catalog
# MAGIC Unity Catalog is a **centralized registry** for all data assets. It provides:
# MAGIC
# MAGIC | Feature | What it does | Distributed Systems Analogy |
# MAGIC |---------|-------------|----------------------------|
# MAGIC | **Namespace** | `catalog.schema.table` | Like DNS: `com.company.service` |
# MAGIC | **Discovery** | "Show me all tables with customer data" | Like a service registry (Consul, ZooKeeper) |
# MAGIC | **Governance** | Row-level security, column masking | Like IAM policies in AWS |
# MAGIC | **Lineage** | "Where did this data come from? What depends on it?" | Like dependency tracking |
# MAGIC | **Volumes** | Store files of any format (CSV, images, models) | Like S3 buckets with access control |
# MAGIC
# MAGIC ```
# MAGIC Unity Catalog Hierarchy:
# MAGIC ├── samples (catalog — pre-loaded, read-only)
# MAGIC │   └── tpch (schema)
# MAGIC │       ├── customer (table — 750K rows)
# MAGIC │       ├── orders   (table — 7.5M rows)
# MAGIC │       └── lineitem (table — 30M rows)
# MAGIC └── workspace (catalog — your writable space)
# MAGIC     └── default (schema)
# MAGIC         ├── your_table (tables you create)
# MAGIC         └── your_volume (files you store)
# MAGIC ```
# MAGIC
# MAGIC > 📖 [Unity Catalog docs](https://docs.databricks.com/aws/en/data-governance/unity-catalog/) | [Volumes](https://docs.databricks.com/aws/en/volumes/)

# COMMAND ----------

# ─── SETUP: Create a Volume for our file experiments ──────────────
# A Volume is a directory in Unity Catalog for storing files
from pyspark.sql.functions import col, round as spark_round, count, sum as spark_sum, avg

print("Creating Unity Catalog Volume for file experiments...")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.format_lab")
VOL = "/Volumes/workspace/default/format_lab"
print(f"  Volume path: {VOL}")
print()
print("Loading source data from TPC-H...")
orders = spark.read.table("samples.tpch.orders")
customers = spark.read.table("samples.tpch.customer")

# Join orders + customers into a rich dataset, then take 100K rows
joined = (orders
    .join(customers, orders.o_custkey == customers.c_custkey)
    .select(
        col("o_orderkey").alias("order_id"),
        col("c_custkey").alias("customer_id"),
        col("c_name").alias("customer_name"),
        col("c_mktsegment").alias("segment"),
        col("c_nationkey").alias("nation_key"),
        col("o_orderdate").alias("order_date"),
        col("o_totalprice").alias("total_price"),
        col("o_orderstatus").alias("status"),
        col("o_orderpriority").alias("priority")
    )
)

sample_df = joined.limit(100000)
row_count = sample_df.count()
print(f"Working dataset: {row_count:,} rows, {len(sample_df.columns)} columns")
print(f"Columns: {sample_df.columns}")
sample_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Format 1: CSV — The "Old Way"
# MAGIC
# MAGIC **Comma-Separated Values** — what most people think of when they think "data file."
# MAGIC
# MAGIC | Trait | Value |
# MAGIC |-------|-------|
# MAGIC | Storage | **Row-based** |
# MAGIC | Schema | **None** — types are guessed or everything is a string |
# MAGIC | Compression | **None** built in |
# MAGIC | Column pruning | **No** — must read entire rows |
# MAGIC | Human-readable | **Yes** — open in Excel or Notepad |
# MAGIC | Best for | Small datasets, data exchange with non-technical users |

# COMMAND ----------

# ─── WRITING AND READING CSV ──────────────────────────────────────
csv_path = f"{VOL}/orders_csv"
sample_df.write.format("csv").mode("overwrite").option("header", "true").save(csv_path)
print(f"Written CSV to: {csv_path}")
print()

# Read it back — notice we must enable inferSchema
csv_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(csv_path)
print(f"Rows read back: {csv_df.count():,}")
print()

# Show schema — types may be wrong!
print("Schema from CSV (types are GUESSED by inferSchema):")
csv_df.printSchema()

# COMMAND ----------

# Let's SEE what the actual CSV files look like on disk
print("FILE LISTING — What Spark actually wrote:")
print("=" * 60)
files = dbutils.fs.ls(csv_path)
for f in files:
    size_kb = f.size / 1024
    print(f"  {f.name:40s}  {size_kb:>8.1f} KB")
print()

# Show the raw content of one CSV file
print("RAW CONTENT of one CSV file (first 500 chars):")
print("-" * 60)
csv_files = [f.path for f in files if f.name.endswith(".csv")]
if csv_files:
    raw = dbutils.fs.head(csv_files[0], 500)
    print(raw)
    print()
print("Notice: Just text! Header row + data rows. No type information.")
print("If this file is corrupted, there is no way to know.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Format 2: Parquet — The Spark Workhorse
# MAGIC
# MAGIC **Apache Parquet** is a **columnar** binary format. It is the default file format
# MAGIC behind most Spark tables and data lake architectures.
# MAGIC
# MAGIC | Trait | Value |
# MAGIC |-------|-------|
# MAGIC | Storage | **Columnar** |
# MAGIC | Schema | **Embedded** — exact types stored in file metadata |
# MAGIC | Compression | **Built-in** (Snappy, ZSTD, GZIP) — often 2-10x smaller than CSV |
# MAGIC | Column pruning | **Yes** — `SELECT name` reads only the name column! |
# MAGIC | Predicate pushdown | **Yes** — `WHERE price > 100` skips irrelevant row groups |
# MAGIC | Human-readable | **No** — binary format |
# MAGIC | Best for | Analytics, data lakes, any read-heavy workload |

# COMMAND ----------

# ─── WRITING AND READING PARQUET ──────────────────────────────────
parquet_path = f"{VOL}/orders_parquet"
sample_df.write.format("parquet").mode("overwrite").save(parquet_path)
print(f"Written Parquet to: {parquet_path}")
print()

parquet_df = spark.read.format("parquet").load(parquet_path)
print(f"Rows read back: {parquet_df.count():,}")
print()

print("Schema from Parquet (types are EXACT — no guessing!):")
parquet_df.printSchema()

# COMMAND ----------

# COLUMN PRUNING DEMO — Parquet's superpower
print("COLUMN PRUNING DEMO")
print("=" * 50)
print("Reading only 2 of 9 columns from Parquet:")
pruned = parquet_df.select("customer_name", "total_price")
pruned.show(5)
print("Parquet physically read ONLY these 2 columns from disk.")
print("CSV would have read all 9 columns and discarded 7.")
print("With 100 columns, this is a ~50x speedup!")

# COMMAND ----------

# Let's SEE the Parquet files
print("FILE LISTING — Parquet on disk:")
print("=" * 60)
files = dbutils.fs.ls(parquet_path)
for f in files:
    size_kb = f.size / 1024
    print(f"  {f.name:40s}  {size_kb:>8.1f} KB")
print()
print("Notice: .parquet files are BINARY (can't open in Notepad)")
print("But they contain embedded schema, statistics, and compressed data.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Format 3: Avro — Row-Based But Better
# MAGIC
# MAGIC **Apache Avro** is a **row-based binary** format with embedded schema.
# MAGIC It is the go-to format for **streaming** and **write-heavy** workloads
# MAGIC (Kafka, event logs, IoT data).
# MAGIC
# MAGIC | Trait | Value |
# MAGIC |-------|-------|
# MAGIC | Storage | **Row-based** (like CSV, but binary) |
# MAGIC | Schema | **Embedded** + excellent schema evolution |
# MAGIC | Compression | **Built-in** |
# MAGIC | Column pruning | **No** — must read full rows |
# MAGIC | Best for | Streaming writes, Kafka, event logs, schema evolution |
# MAGIC
# MAGIC **When to use Avro vs Parquet?**
# MAGIC - **Writing new events one at a time?** → Avro (row-based = cheap appends)
# MAGIC - **Querying analytics across millions of rows?** → Parquet (columnar = read only what you need)

# COMMAND ----------

# ─── WRITING AND READING AVRO ─────────────────────────────────────
avro_path = f"{VOL}/orders_avro"
sample_df.write.format("avro").mode("overwrite").save(avro_path)
print(f"Written Avro to: {avro_path}")
print()

avro_df = spark.read.format("avro").load(avro_path)
print(f"Rows read back: {avro_df.count():,}")
print()

print("KEY COMPARISON:")
print("  Parquet = COLUMNAR -> great for reading/analytics")
print("  Avro    = ROW-BASED -> great for writing/streaming")
print()
print("  Use Parquet when: you query specific columns across many rows")
print("  Use Avro when: you append many records quickly (Kafka, event logs)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Format 4: Delta Lake — The Game Changer
# MAGIC
# MAGIC Delta Lake is NOT a file format — it is a **table format** built ON TOP of Parquet files.
# MAGIC It adds the features that Parquet is missing for production data systems:
# MAGIC
# MAGIC | Feature | Parquet | Delta Lake |
# MAGIC |---------|--------|------------|
# MAGIC | ACID transactions | No — crash mid-write = corruption | Yes — writes are atomic |
# MAGIC | UPDATE/DELETE rows | No — must rewrite entire file | Yes — with transaction log |
# MAGIC | Schema enforcement | No — anything goes | Yes — rejects bad types |
# MAGIC | Time travel | No — old data is gone | Yes — query any previous version |
# MAGIC | Transaction log | No | Yes — `_delta_log/` tracks every change |
# MAGIC
# MAGIC **How it works under the hood**:
# MAGIC ```
# MAGIC my_table/
# MAGIC ├── _delta_log/           <-- Transaction log (JSON files)
# MAGIC │   ├── 000000.json       <-- Version 0: "CREATE TABLE, added files: [part-001.parquet, part-002.parquet]"
# MAGIC │   ├── 000001.json       <-- Version 1: "UPDATE 500 rows, added: [part-003.parquet], removed: [part-001.parquet]"
# MAGIC │   └── 000002.json       <-- Version 2: "DELETE WHERE status='cancelled'"
# MAGIC ├── part-001.snappy.parquet   <-- Actual data (standard Parquet files!)
# MAGIC ├── part-002.snappy.parquet
# MAGIC └── part-003.snappy.parquet
# MAGIC ```
# MAGIC
# MAGIC **Delta Lake = Parquet files + a transaction log directory.**
# MAGIC This is the **default** format in Databricks.
# MAGIC
# MAGIC > 📖 [Delta Lake docs](https://docs.delta.io/) | [DESCRIBE HISTORY](https://docs.databricks.com/aws/en/delta/history.html) | [Time Travel](https://docs.databricks.com/aws/en/delta/history.html#query-table-version)

# COMMAND ----------

# ─── WRITING A DELTA TABLE ────────────────────────────────────────
delta_table_name = "workspace.default.orders_demo"
sample_df.write.format("delta").mode("overwrite").saveAsTable(delta_table_name)
print(f"Written Delta table: {delta_table_name}")

delta_df = spark.read.table(delta_table_name)
print(f"Rows read back: {delta_df.count():,}")
print()
print("Schema (exact types — ENFORCED on every write):")
delta_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Lake Feature: Time Travel
# MAGIC
# MAGIC Every write to a Delta table creates a new **version**.
# MAGIC You can query any previous version — like **Git for data**.
# MAGIC
# MAGIC ```python
# MAGIC # Read current version
# MAGIC df = spark.read.table("my_table")
# MAGIC
# MAGIC # Read version 0 (the original data)
# MAGIC df_v0 = spark.read.format("delta").option("versionAsOf", 0).table("my_table")
# MAGIC
# MAGIC # Read data as of yesterday
# MAGIC df_yesterday = spark.read.format("delta").option("timestampAsOf", "2026-02-24").table("my_table")
# MAGIC ```

# COMMAND ----------

# ─── TIME TRAVEL DEMO ─────────────────────────────────────────────
print("DELTA LAKE — TIME TRAVEL")
print("=" * 60)

# Step 1: See the current history
print("Table history (version 0 = initial write):")
history = spark.sql(f"DESCRIBE HISTORY {delta_table_name}")
history.select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

# Step 2: Make a change — UPDATE some rows
print("Updating all '1-URGENT' orders to priority 'CRITICAL'...")
spark.sql("UPDATE " + delta_table_name + " SET priority = 'CRITICAL' WHERE priority = '1-URGENT'")

critical_count = spark.sql("SELECT COUNT(*) as cnt FROM " + delta_table_name + " WHERE priority = 'CRITICAL'").collect()[0].cnt
print(f"  Updated: {critical_count:,} orders are now CRITICAL")

# Step 3: See the new history — now there are 2 versions
print()
print("Updated history (now 2 versions):")
history2 = spark.sql(f"DESCRIBE HISTORY {delta_table_name}")
history2.select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

# Step 4: TIME TRAVEL — read the OLD version
print("TIME TRAVEL — Reading version 0 (BEFORE the update):")
old_df = spark.read.format("delta").option("versionAsOf", 0).table(delta_table_name)
old_urgent = old_df.filter(old_df.priority == "1-URGENT").count()
print(f"  Version 0 had {old_urgent:,} '1-URGENT' orders")

new_df = spark.read.table(delta_table_name)
new_urgent = new_df.filter(new_df.priority == "1-URGENT").count()
new_critical = new_df.filter(new_df.priority == "CRITICAL").count()
print(f"  Current version: {new_urgent:,} URGENT, {new_critical:,} CRITICAL")
print()
print("  The old data is STILL THERE! Delta keeps every version.")
print("  This is impossible with plain CSV or Parquet files.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Lake Feature: Schema Enforcement
# MAGIC
# MAGIC Delta Lake **rejects** writes that don't match the table's schema.
# MAGIC This prevents "garbage in" problems — a string value cannot sneak into an integer column.

# COMMAND ----------

# ─── SCHEMA ENFORCEMENT DEMO ─────────────────────────────────────
print("DELTA LAKE — SCHEMA ENFORCEMENT")
print("=" * 60)

# Create deliberately bad data
bad_data = spark.createDataFrame(
    [("not_a_number", "Alice", 99.99)],
    ["order_id", "customer_name", "total_price"]
)
print("Attempting to append data with wrong types...")
print("  bad_data: order_id='not_a_number' (STRING)")
print("  table:    order_id=BIGINT")
print()

try:
    bad_data.write.format("delta").mode("append").saveAsTable(delta_table_name)
    print("  Unexpected: write succeeded")
except Exception as e:
    print(f"  REJECTED! Delta enforced the schema.")
    print(f"  Error: {str(e)[:200]}")
print()
print("In CSV or Parquet, this bad data would silently enter the table.")
print("Delta Lake catches type mismatches BEFORE any data is written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Format Comparison Summary
# MAGIC
# MAGIC | Feature | CSV | Parquet | Avro | Delta Lake |
# MAGIC |---------|-----|---------|------|------------|
# MAGIC | **Storage** | Row-based | **Columnar** | Row-based | **Columnar** (Parquet underneath) |
# MAGIC | **Schema** | None | Embedded | Embedded | **Enforced** |
# MAGIC | **Types** | Guessed | Exact | Exact | Exact |
# MAGIC | **Compression** | None | Built-in | Built-in | Built-in |
# MAGIC | **Column pruning** | No | **Yes** | No | **Yes** |
# MAGIC | **ACID transactions** | No | No | No | **Yes** |
# MAGIC | **UPDATE/DELETE** | No | No | No | **Yes** |
# MAGIC | **Time travel** | No | No | No | **Yes** |
# MAGIC | **Human-readable** | Yes | No | No | No |
# MAGIC | **Best for** | Small data, exchange | Analytics | Streaming, writes | **Production data systems** |
# MAGIC
# MAGIC **The progression**:
# MAGIC - CSV: *"I can read it in Notepad"*
# MAGIC - Parquet: *"My queries are 10x faster"*
# MAGIC - Delta Lake: *"My data has transactions, versioning, and governance"*

# COMMAND ----------

# ─── VERIFY: All formats side by side ─────────────────────────────
print("FORMAT VERIFICATION — All formats readable:")
print("=" * 60)
csv_count = spark.read.format("csv").option("header","true").load(f"{VOL}/orders_csv").count()
parquet_count = spark.read.format("parquet").load(f"{VOL}/orders_parquet").count()
avro_count = spark.read.format("avro").load(f"{VOL}/orders_avro").count()
delta_count = spark.read.table(delta_table_name).count()

print(f"  CSV:     {csv_count:>10,} rows")
print(f"  Parquet: {parquet_count:>10,} rows")
print(f"  Avro:    {avro_count:>10,} rows")
print(f"  Delta:   {delta_count:>10,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

print("Cleaning up test data...")
spark.sql("DROP TABLE IF EXISTS workspace.default.orders_demo")
spark.sql("DROP VOLUME IF EXISTS workspace.default.format_lab")
print("  All test data removed (tables and volume).")
print("  NOTEBOOK COMPLETE!")
