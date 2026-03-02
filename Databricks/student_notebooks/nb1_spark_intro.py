# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1: Introduction to Apache Spark & DataFrames
# MAGIC 
# MAGIC **Course**: Distributed Systems for Data Science  
# MAGIC **Week**: 7 — Spark & the Lakehouse
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What You Will Learn
# MAGIC 
# MAGIC In your databases course, you worked with a **single-server** database: one machine, one disk, one query engine.
# MAGIC That works for millions of rows — but what about **billions**? What if the data doesn't fit on one machine?
# MAGIC
# MAGIC **Apache Spark** solves this by distributing data and computation across a cluster of machines.
# MAGIC Instead of one server scanning a table, Spark splits the table into **partitions** and assigns
# MAGIC each partition to a different worker. All workers process their partitions **in parallel**.
# MAGIC
# MAGIC | Concept | Traditional DB | Spark |
# MAGIC |---------|---------------|-------|
# MAGIC | Data location | One server | Distributed across workers |
# MAGIC | Query execution | Single thread (mostly) | Parallel across cluster |
# MAGIC | Data format | Proprietary (MySQL pages, Postgres heap) | Open formats (Parquet, Delta, Iceberg) |
# MAGIC | Scaling | Buy a bigger server (vertical) | Add more workers (horizontal) |
# MAGIC | API | SQL only | SQL + Python + Java + Scala |
# MAGIC
# MAGIC ### Key Spark Concepts
# MAGIC
# MAGIC - **DataFrame**: A distributed table. Looks like a pandas DataFrame, but lives across multiple machines.
# MAGIC - **Transformation**: An operation that describes *what* to do (filter, join, group by) — but **does NOT execute yet**. 
# MAGIC - **Action**: An operation that triggers actual execution (count, show, collect). This is when Spark builds the plan and runs it.
# MAGIC - **Lazy Evaluation**: Spark waits until an action before doing any work. This lets the optimizer combine and rearrange transformations for efficiency.
# MAGIC - **DAG (Directed Acyclic Graph)**: The execution plan — a graph of stages and tasks that Spark builds from your transformations.
# MAGIC
# MAGIC ### Databricks-Specific
# MAGIC
# MAGIC - **Serverless Compute**: Your cluster is managed for you. No configuration needed — just run code.
# MAGIC - **Unity Catalog**: The central registry for all data assets (tables, schemas, permissions). Think of it as **DNS for data**.
# MAGIC - **Query Profile**: A visual DAG of your query's execution — click "See performance" after any cell runs.
# MAGIC
# MAGIC > 📖 **API Reference**: [PySpark DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html) | [PySpark Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html) | [Databricks Query Profile](https://docs.databricks.com/aws/en/compute/sql-warehouse/query-profile.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Loading Data — Your First DataFrame
# MAGIC
# MAGIC In your databases class, you loaded data with `INSERT INTO` or imported CSVs.
# MAGIC In Spark, data lives in **tables** managed by **Unity Catalog** — a three-level namespace:
# MAGIC
# MAGIC ```
# MAGIC catalog.schema.table
# MAGIC └─ samples.tpch.customer
# MAGIC    ├── catalog = "samples" (a collection of schemas)
# MAGIC    ├── schema  = "tpch"    (a collection of tables)
# MAGIC    └── table   = "customer" (the actual data)
# MAGIC ```
# MAGIC
# MAGIC Loading a table is **one line**. Spark distributes it across workers automatically.

# COMMAND ----------

# ─── LOADING A TABLE ──────────────────────────────────────────────
# spark.read.table() reads from Unity Catalog
# The data is DISTRIBUTED — split across Spark workers automatically

customers = spark.read.table("samples.tpch.customer")

# .printSchema() shows column names and types — no data moves yet
print("SCHEMA — samples.tpch.customer")
print("=" * 50)
customers.printSchema()

# .count() is an ACTION — this triggers actual distributed execution
total = customers.count()
print(f"Total customers: {total:,}")
print()
print("KEY TAKEAWAYS:")
print(f"  1. spark.read.table() loaded {total:,} rows — distributed across workers")
print(f"  2. printSchema() showed the structure WITHOUT reading any data")
print(f"  3. .count() is an ACTION — it triggered actual execution")
print(f"  4. Notice: types are exact (long, string, decimal) — not guessed like CSV")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 👆 What Just Happened?
# MAGIC 
# MAGIC 1. `spark.read.table("samples.tpch.customer")` — told Spark WHERE the data is (Unity Catalog)
# MAGIC 2. `printSchema()` — read only the **metadata** (column names/types), not the actual data
# MAGIC 3. `.count()` — this is an **ACTION** that triggered real work:
# MAGIC    - Spark built an execution plan
# MAGIC    - Distributed the scan across workers
# MAGIC    - Each worker counted its partition
# MAGIC    - Results were combined and returned
# MAGIC
# MAGIC 💡 **Click "See performance" above** to view the **Query Profile** — a visual DAG showing
# MAGIC how Spark executed your count. You'll see a single scan operation with the number of rows processed.
# MAGIC
# MAGIC > 📖 [spark.read.table()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.table.html) | [Query Profile docs](https://docs.databricks.com/aws/en/compute/sql-warehouse/query-profile.html)

# COMMAND ----------

# ─── LOOKING AT THE DATA ──────────────────────────────────────────
# display() is Databricks-specific: renders data as an interactive table
# with sorting, filtering, and built-in charting

display(customers.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC #### About `display()` vs `.show()`
# MAGIC
# MAGIC | Method | What it does | Where it works |
# MAGIC |--------|-------------|----------------|
# MAGIC | `display(df)` | Rich interactive table + charts | Databricks only |
# MAGIC | `df.show(n)` | Text-based table printed to stdout | Any Spark environment |
# MAGIC | `df.collect()` | Returns all rows as Python list of `Row` objects to driver memory | Any Spark ⚠️ |
# MAGIC | `df.toPandas()` | Converts to pandas DataFrame in driver memory | Any Spark ⚠️ |
# MAGIC
# MAGIC ⚠️ **Warning**: `.collect()` and `.toPandas()` bring ALL data to a single machine.
# MAGIC On large datasets this will crash with Out of Memory. Always `.limit()` first!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Transformations — Lazy Evaluation
# MAGIC
# MAGIC **The most important concept in Spark**: Transformations are **lazy**.
# MAGIC
# MAGIC When you write `df.filter(...)` or `df.select(...)`, Spark does NOT execute anything.
# MAGIC It just records your intent in a plan. Only when you call an **action** (like `.count()`,
# MAGIC `.show()`, `display()`) does Spark actually process data.
# MAGIC
# MAGIC **Why?** Because the optimizer can see your entire chain of transformations and rearrange,
# MAGIC combine, and optimize them before executing. This is like a SQL query planner, but for code.
# MAGIC
# MAGIC ```
# MAGIC Transformations (LAZY — build the plan):    Actions (EAGER — execute the plan):
# MAGIC   .filter()                                    .count()
# MAGIC   .select()                                    .show()
# MAGIC   .groupBy()                                   .collect()
# MAGIC   .join()                                      display()
# MAGIC   .withColumn()                                .toPandas()
# MAGIC   .orderBy()                                   .write.save()
# MAGIC ```

# COMMAND ----------

# ─── LAZY EVALUATION DEMO ─────────────────────────────────────────
# Watch: these two lines are INSTANT — no data is read

building_customers = customers.filter(customers.c_mktsegment == "BUILDING")
selected = building_customers.select("c_custkey", "c_name", "c_mktsegment", "c_acctbal")

print("LAZY EVALUATION DEMO")
print("=" * 50)
print("1. customers.filter(...)  <-- TRANSFORMATION (instant, builds plan)")
print("2. .select(...)           <-- TRANSFORMATION (instant, extends plan)")
print()
print("Nothing has executed yet! Now calling an ACTION:")
print()

# THIS is when Spark actually reads data, applies filter, and selects columns
count = selected.count()
print(f"3. .count()               <-- ACTION (triggered execution)")
print(f"   Result: {count:,} BUILDING customers found out of {total:,} total")
print()
print("Spark only NOW scanned the data, applied the filter, and counted.")
print("It also applied COLUMN PRUNING — only read the 4 columns we selected,")
print("not all 8 columns in the table.")

# COMMAND ----------

# Let's see the actual data
print("First 10 BUILDING customers:")
selected.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 👆 What Just Happened?
# MAGIC
# MAGIC Spark combined `filter` + `select` + `count` into a single optimized plan:
# MAGIC 1. **Scan** only 4 columns (column pruning — skipped 4 unnecessary columns)
# MAGIC 2. **Filter** rows where segment = "BUILDING" (predicate pushdown — done during scan)
# MAGIC 3. **Count** the remaining rows
# MAGIC
# MAGIC 💡 **Click "See performance"** on the `.count()` cell to see this optimization in the Query Profile.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Aggregations — GroupBy, Agg, OrderBy
# MAGIC
# MAGIC `GROUP BY` works just like SQL, but the syntax is chained method calls.
# MAGIC This is where Spark's distributed nature starts to shine — aggregating
# MAGIC **millions of rows** across parallel workers.
# MAGIC
# MAGIC Under the hood, aggregation triggers a **shuffle**: Spark redistributes data
# MAGIC so that all rows with the same key end up on the same worker. This is expensive
# MAGIC but necessary — you can't compute `SUM(price) GROUP BY segment` if rows from the
# MAGIC same segment are scattered across different machines.
# MAGIC
# MAGIC > 📖 [groupBy()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html) | [agg()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.agg.html)

# COMMAND ----------

from pyspark.sql.functions import count, sum, avg, round as spark_round, col, desc

# ─── AGGREGATION: Market Segments ─────────────────────────────────
segments = (customers
    .groupBy("c_mktsegment")
    .agg(
        count("*").alias("num_customers"),
        spark_round(avg("c_acctbal"), 2).alias("avg_balance")
    )
    .orderBy(desc("num_customers"))
)

print("AGGREGATION: Customer count and average balance by market segment")
print("=" * 60)
segments.show()

print("KEY TAKEAWAYS:")
print("  1. groupBy() triggered a SHUFFLE — data redistributed by segment")
print("  2. agg() computed count + avg in parallel across workers")
print("  3. Each segment has ~150K customers — the data is evenly distributed")
print()
print("Click 'See performance' above to see the SHUFFLE in the Query Profile!")

# COMMAND ----------

# ─── BIGGER AGGREGATION: Orders table (7.5M rows) ─────────────────
orders = spark.read.table("samples.tpch.orders")

order_stats = (orders
    .groupBy("o_orderstatus")
    .agg(
        count("*").alias("order_count"),
        spark_round(sum("o_totalprice"), 2).alias("total_value"),
        spark_round(avg("o_totalprice"), 2).alias("avg_value")
    )
    .orderBy(desc("total_value"))
)

order_count = orders.count()
print(f"Aggregating {order_count:,} orders by status...")
print("=" * 60)
order_stats.show()

print("STATUS CODES:")
print("  F = Fulfilled, O = Open, P = Partial")
print(f"  Total orders: {order_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Joins — Distributed Data Matching
# MAGIC
# MAGIC Joins are the most expensive operation in distributed computing.
# MAGIC Spark must match rows from two tables that live on different machines.
# MAGIC
# MAGIC Strategies Spark uses (visible in the Query Profile):
# MAGIC - **Broadcast Join**: If one table is small, Spark copies it to every worker (fast!)
# MAGIC - **Sort-Merge Join**: Both tables are sorted by the join key, then merged (reliable for large tables)
# MAGIC - **Shuffle Hash Join**: Both tables are shuffled by the join key (middle ground)
# MAGIC
# MAGIC The optimizer picks the strategy automatically based on table sizes.

# COMMAND ----------

# ─── JOIN: Customers x Orders (750K x 7.5M) ──────────────────────
print(f"Joining {total:,} customers with {order_count:,} orders...")
print("=" * 60)

top_spenders = (orders
    .join(customers, orders.o_custkey == customers.c_custkey)
    .groupBy("c_name", "c_mktsegment")
    .agg(
        count("o_orderkey").alias("num_orders"),
        spark_round(sum("o_totalprice"), 2).alias("total_spent")
    )
    .orderBy(desc("total_spent"))
    .limit(10)
)

print("Top 10 customers by total spend:")
top_spenders.show(truncate=False)

print("Click 'See performance' above to see:")
print("  - The JOIN strategy Spark chose (broadcast vs sort-merge)")
print("  - The SHUFFLE that redistributed data by customer key")
print("  - How many rows each operator processed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: The Execution Plan — df.explain()
# MAGIC
# MAGIC Every Spark query goes through four stages of planning:
# MAGIC
# MAGIC 1. **Parsed Logical Plan** — what you wrote (raw AST)
# MAGIC 2. **Analyzed Logical Plan** — resolved table/column names against the catalog
# MAGIC 3. **Optimized Logical Plan** — after the Catalyst optimizer applies rules (predicate pushdown, column pruning, join reordering)
# MAGIC 4. **Physical Plan** — the actual execution strategy (which join algorithm, which scan method, where to shuffle)
# MAGIC
# MAGIC You can see all four with `df.explain(True)`. Read the physical plan **bottom-up** — that's the execution order.
# MAGIC
# MAGIC > 📖 [explain()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html) | [Catalyst Optimizer](https://spark.apache.org/docs/latest/sql-performance-tuning.html)

# COMMAND ----------

# ─── THE EXECUTION PLAN ──────────────────────────────────────────
lineitem = spark.read.table("samples.tpch.lineitem")

result = (lineitem
    .groupBy("l_returnflag", "l_linestatus")
    .agg(
        count("*").alias("cnt"),
        spark_round(avg("l_quantity"), 2).alias("avg_qty"),
        spark_round(sum("l_extendedprice"), 2).alias("sum_price")
    )
    .orderBy("l_returnflag", "l_linestatus")
)

print("EXECUTION PLAN for: aggregate 30 MILLION rows")
print("=" * 60)
print()
result.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 👆 Reading the Physical Plan (Bottom-Up)
# MAGIC
# MAGIC ```
# MAGIC Sort [l_returnflag, l_linestatus]           ← 5. Sort the 4 result rows
# MAGIC   └─ HashAggregate [cnt, avg_qty, sum_price] ← 4. Final aggregation after shuffle
# MAGIC       └─ Exchange (SHUFFLE)                   ← 3. Redistribute by (flag, status)
# MAGIC           └─ HashAggregate (partial)           ← 2. Each worker aggregates its LOCAL data
# MAGIC               └─ Scan lineitem                 ← 1. Read 30M rows (distributed)
# MAGIC ```
# MAGIC
# MAGIC **The key insight**: Step 2 happens **on each worker independently** (partial aggregation).
# MAGIC Then Step 3 shuffles the partial results so all rows with the same key land on one worker.
# MAGIC Step 4 does the final merge. This is the MapReduce pattern: **Map** (partial agg) → **Shuffle** → **Reduce** (final agg).

# COMMAND ----------

# Now actually run it:
lineitem_count = lineitem.count()
print(f"RESULT: {lineitem_count:,} rows aggregated into:")
print("=" * 60)
result.show()

print("WHAT TO LOOK FOR in 'See performance':")
print("  - 'Aggregated task time' >> 'Wall-clock time' = PARALLELISM!")
print("    (multiple workers ran simultaneously)")
print("  - The Exchange (shuffle) shows data moving between workers")
print("  - The partial vs final HashAggregate shows the two-phase pattern")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Creating DataFrames from Scratch
# MAGIC
# MAGIC You can also create DataFrames from local Python data.
# MAGIC `spark.createDataFrame()` takes your Python data and **distributes** it across the cluster.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Method 1: Simple — list of tuples + column names
simple_df = spark.createDataFrame(
    [("Alice", 95), ("Bob", 87), ("Carol", 92)],
    ["name", "score"]
)
print("Method 1: createDataFrame(data, column_names)")
simple_df.show()

# Method 2: Explicit schema (precise type control)
schema = StructType([
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True)
])
products_df = spark.createDataFrame([
    ("Widget A", 100, 9.99),
    ("Widget B", 250, 14.50),
    ("Widget C", 50, 29.99)
], schema)

print("Method 2: createDataFrame(data, StructType schema)")
products_df.show()
products_df.printSchema()

print("NOTE: createDataFrame() distributes local Python data across Spark workers.")
print("For large datasets, always use spark.read.table() or spark.read.format() instead.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Now You Try!
# MAGIC
# MAGIC Complete these exercises using what you've learned. Each one builds on the previous.
# MAGIC After each exercise, **click "See performance"** to examine the Query Profile.
# MAGIC
# MAGIC ### Exercise 1: Supplier Analysis
# MAGIC Using `samples.tpch.supplier` and `samples.tpch.nation`:
# MAGIC - Join them on `s_nationkey = n_nationkey`
# MAGIC - Find the **top 5 nations** by number of suppliers
# MAGIC - Run `.explain(True)` to see the join strategy Spark chose
# MAGIC
# MAGIC ### Exercise 2: Order Trends
# MAGIC Using `samples.tpch.orders`:
# MAGIC - Add a column `order_year` using `year("o_orderdate")`
# MAGIC - Group by `order_year` and calculate: count of orders, average order value
# MAGIC - Order by year
# MAGIC - Hint: `from pyspark.sql.functions import year`
# MAGIC
# MAGIC ### Exercise 3: Revenue by Part Type
# MAGIC Using `samples.tpch.lineitem` (30M rows) and `samples.tpch.part` (1M rows):
# MAGIC - Join on `l_partkey = p_partkey`
# MAGIC - Group by `p_type` and sum `l_extendedprice` as `total_revenue`
# MAGIC - Find the **top 10 part types** by revenue
# MAGIC - Run `.explain(True)` — what join strategy did Spark use for a 30M x 1M join?

# COMMAND ----------

# EXERCISE 1: Your code here
# suppliers = spark.read.table("samples.tpch.supplier")
# nations = spark.read.table("samples.tpch.nation")
# ... your solution ...

# COMMAND ----------

# EXERCISE 2: Your code here
# from pyspark.sql.functions import year
# orders = spark.read.table("samples.tpch.orders")
# ... your solution ...

# COMMAND ----------

# EXERCISE 3: Your code here
# lineitem = spark.read.table("samples.tpch.lineitem")
# parts = spark.read.table("samples.tpch.part")
# ... your solution ...
