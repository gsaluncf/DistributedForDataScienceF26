# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 3: SQL on Spark
# MAGIC 
# MAGIC **Course**: Distributed Systems for Data Science  
# MAGIC **Week**: 7 — Spark & the Lakehouse
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The Bridge Between What You Know and What Spark Does
# MAGIC
# MAGIC You already know SQL from your databases course. Good news: **Spark speaks SQL**.
# MAGIC Every SQL query you write in Databricks is compiled into a distributed execution plan
# MAGIC and run across multiple workers — the same engine that powers DataFrames.
# MAGIC
# MAGIC In fact, DataFrames and SQL are two syntaxes for the **exact same engine**:
# MAGIC
# MAGIC ```python
# MAGIC # DataFrame API:
# MAGIC df.filter(df.segment == "BUILDING").groupBy("status").count()
# MAGIC
# MAGIC # Equivalent SQL:
# MAGIC SELECT status, COUNT(*) FROM table WHERE segment = 'BUILDING' GROUP BY status
# MAGIC ```
# MAGIC
# MAGIC Both produce the **identical** execution plan. Neither is "faster" — they compile to the same DAG.
# MAGIC
# MAGIC ### Three Ways to Use SQL in Databricks
# MAGIC
# MAGIC | Method | When to use |
# MAGIC |--------|-------------|
# MAGIC | `%sql` magic cell | Pure SQL — great for exploration, dashboards |
# MAGIC | `spark.sql("...")` | SQL from Python — get a DataFrame back, chain further operations |
# MAGIC | Temp views | Bridge: create a DataFrame in Python, query it with SQL |
# MAGIC
# MAGIC > 📖 [Spark SQL Reference](https://spark.apache.org/docs/latest/sql-ref.html) | [Databricks SQL Guide](https://docs.databricks.com/aws/en/sql/) | [Query Profile](https://docs.databricks.com/aws/en/compute/sql-warehouse/query-profile.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Direct SQL — The `%sql` Magic
# MAGIC
# MAGIC A cell starting with `%sql` runs **pure SQL**. The results appear as an interactive table
# MAGIC with built-in sorting, filtering, and visualization (click the chart icons below the results).
# MAGIC
# MAGIC This is the fastest way to explore data — no Python needed.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Market segment analysis: how many customers per segment, and their average balance?
# MAGIC -- This query runs on 750,000 rows distributed across Spark workers.
# MAGIC
# MAGIC SELECT 
# MAGIC     c_mktsegment AS segment,
# MAGIC     COUNT(*) AS num_customers,
# MAGIC     ROUND(AVG(c_acctbal), 2) AS avg_balance,
# MAGIC     ROUND(MIN(c_acctbal), 2) AS min_balance,
# MAGIC     ROUND(MAX(c_acctbal), 2) AS max_balance
# MAGIC FROM samples.tpch.customer
# MAGIC GROUP BY c_mktsegment
# MAGIC ORDER BY num_customers DESC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 👆 What to Notice
# MAGIC
# MAGIC 1. **Interactive table**: Click column headers to sort, drag to resize
# MAGIC 2. **Visualization**: Click the **📊 chart icon** below results → try a bar chart of `segment` vs `num_customers`
# MAGIC 3. **Query Profile**: Click **"See performance"** to see the distributed execution plan
# MAGIC 4. The query scanned **750,000 rows** — each worker processed its partition in parallel
# MAGIC
# MAGIC > 💡 **Try it**: Click the `+` icon next to the result table and select "Visualization" to create a chart!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Another example: Orders by status with financial summary
# MAGIC -- This scans 7.5 MILLION orders — watch how fast it is on distributed compute
# MAGIC
# MAGIC SELECT 
# MAGIC     o_orderstatus AS status,
# MAGIC     CASE o_orderstatus
# MAGIC         WHEN 'F' THEN 'Fulfilled'
# MAGIC         WHEN 'O' THEN 'Open'
# MAGIC         WHEN 'P' THEN 'Partial'
# MAGIC     END AS status_label,
# MAGIC     COUNT(*) AS order_count,
# MAGIC     ROUND(SUM(o_totalprice), 2) AS total_value,
# MAGIC     ROUND(AVG(o_totalprice), 2) AS avg_order_value,
# MAGIC     ROUND(STDDEV(o_totalprice), 2) AS stddev_value
# MAGIC FROM samples.tpch.orders
# MAGIC GROUP BY o_orderstatus
# MAGIC ORDER BY order_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: `spark.sql()` — SQL from Python
# MAGIC
# MAGIC `spark.sql()` runs a SQL query and returns the result as a **DataFrame**.
# MAGIC This is the best of both worlds: write the query in SQL, then process the result with Python.
# MAGIC
# MAGIC ```python
# MAGIC # Run SQL, get a DataFrame
# MAGIC result_df = spark.sql("SELECT * FROM table WHERE price > 100")
# MAGIC
# MAGIC # Now use DataFrame operations on the result
# MAGIC result_df.filter(result_df.quantity > 10).show()
# MAGIC ```

# COMMAND ----------

# ─── spark.sql() EXAMPLE ─────────────────────────────────────────
# SQL query returns a DataFrame — chain further operations in Python!

result_df = spark.sql(
    "SELECT n.n_name AS nation, COUNT(s.s_suppkey) AS num_suppliers, "
    "ROUND(AVG(s.s_acctbal), 2) AS avg_balance "
    "FROM samples.tpch.supplier s "
    "JOIN samples.tpch.nation n ON s.s_nationkey = n.n_nationkey "
    "GROUP BY n.n_name ORDER BY num_suppliers DESC LIMIT 10"
)

print("spark.sql() returns a DataFrame:")
print(f"  Type: {type(result_df)}")
print(f"  Columns: {result_df.columns}")
print()
result_df.show()

# You can keep working with it in Python:
positive = result_df.filter(result_df.avg_balance > 0)
print(f"Nations with positive avg balance: {positive.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 👆 Key Insight
# MAGIC
# MAGIC The result of `spark.sql()` is a **regular DataFrame**. You can:
# MAGIC - `.filter()`, `.select()`, `.groupBy()` it
# MAGIC - `.show()`, `display()`, `.toPandas()` it
# MAGIC - Write it to a table with `.write.saveAsTable()`
# MAGIC - Join it with other DataFrames
# MAGIC
# MAGIC This makes `spark.sql()` the **bridge** between SQL and Python.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Temp Views — The Two-Way Bridge
# MAGIC
# MAGIC What if you built a complex DataFrame in Python and want to query it with SQL?
# MAGIC **Temp views** let you register a DataFrame as a "virtual table" that SQL can see.
# MAGIC
# MAGIC ```python
# MAGIC # Step 1: Build a DataFrame in Python
# MAGIC my_df = orders.join(customers, ...).select(...)
# MAGIC
# MAGIC # Step 2: Register as a temp view
# MAGIC my_df.createOrReplaceTempView("my_view")
# MAGIC
# MAGIC # Step 3: Query with SQL!
# MAGIC spark.sql("SELECT * FROM my_view WHERE price > 100")
# MAGIC ```
# MAGIC
# MAGIC The temp view lives only in your current session — it's not saved to the catalog.

# COMMAND ----------

# ─── TEMP VIEW EXAMPLE ────────────────────────────────────────────
# Step 1: Build a DataFrame with Python (join orders + customers)
orders = spark.read.table("samples.tpch.orders")
customers = spark.read.table("samples.tpch.customer")

customer_orders = (orders
    .join(customers, orders.o_custkey == customers.c_custkey)
    .select("c_name", "c_mktsegment", "o_orderdate", "o_totalprice", "o_orderstatus")
)

# Step 2: Register as a temp view
customer_orders.createOrReplaceTempView("customer_orders_view")
print(f"Created temp view 'customer_orders_view' with {customer_orders.count():,} rows")

# Step 3: Query with SQL!
print()
sql_result = spark.sql(
    "SELECT c_mktsegment, o_orderstatus, COUNT(*) AS order_count, "
    "ROUND(AVG(o_totalprice), 2) AS avg_price "
    "FROM customer_orders_view "
    "GROUP BY c_mktsegment, o_orderstatus "
    "ORDER BY c_mktsegment, order_count DESC"
)
sql_result.show(15)

print("Built with Python DataFrame API -> Queried with SQL!")
print("The temp view exists only in this notebook session.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Common Table Expressions (CTEs)
# MAGIC
# MAGIC CTEs make complex queries readable by breaking them into named steps.
# MAGIC They work exactly the same in Spark SQL as in PostgreSQL or MySQL.
# MAGIC
# MAGIC ```sql
# MAGIC WITH step1 AS (
# MAGIC     SELECT ... -- compute something
# MAGIC ),
# MAGIC step2 AS (
# MAGIC     SELECT ... FROM step1 -- use step1's result
# MAGIC )
# MAGIC SELECT ... FROM step2  -- final result
# MAGIC ```
# MAGIC
# MAGIC The query below finds customers whose average order value is **50% above** the global average.
# MAGIC Two CTEs keep the logic clean and readable.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CTE EXAMPLE: Customers with above-average order values
# MAGIC -- CTE 1: Per-customer stats
# MAGIC -- CTE 2: Global average
# MAGIC -- Final: Compare each customer to the global average
# MAGIC
# MAGIC WITH customer_stats AS (
# MAGIC     SELECT 
# MAGIC         o.o_custkey,
# MAGIC         c.c_name,
# MAGIC         c.c_mktsegment,
# MAGIC         COUNT(*) AS num_orders,
# MAGIC         ROUND(AVG(o.o_totalprice), 2) AS avg_order_value,
# MAGIC         ROUND(SUM(o.o_totalprice), 2) AS total_spent
# MAGIC     FROM samples.tpch.orders o
# MAGIC     JOIN samples.tpch.customer c ON o.o_custkey = c.c_custkey
# MAGIC     GROUP BY o.o_custkey, c.c_name, c.c_mktsegment
# MAGIC ),
# MAGIC overall AS (
# MAGIC     SELECT ROUND(AVG(avg_order_value), 2) AS global_avg
# MAGIC     FROM customer_stats
# MAGIC )
# MAGIC SELECT 
# MAGIC     cs.c_name,
# MAGIC     cs.c_mktsegment,
# MAGIC     cs.num_orders,
# MAGIC     cs.avg_order_value,
# MAGIC     cs.total_spent,
# MAGIC     o.global_avg,
# MAGIC     ROUND(cs.avg_order_value / o.global_avg * 100 - 100, 1) AS pct_above_avg
# MAGIC FROM customer_stats cs
# MAGIC CROSS JOIN overall o
# MAGIC WHERE cs.avg_order_value > o.global_avg * 1.5
# MAGIC ORDER BY cs.total_spent DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC #### 👆 Reading the CTE
# MAGIC
# MAGIC | Step | What it does |
# MAGIC |------|-------------|
# MAGIC | `customer_stats` | Joins orders+customers, computes per-customer: # orders, avg value, total spent |
# MAGIC | `overall` | Computes the global average order value (one number) |
# MAGIC | Final SELECT | Joins each customer's stats with the global avg, filters to those 50%+ above |
# MAGIC
# MAGIC 💡 **Click "See performance"** — notice the CTE compiles to a single optimized plan, not separate queries.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Window Functions — Analytics Powerhouse
# MAGIC
# MAGIC Window functions are the most powerful SQL feature for analytics. They calculate 
# MAGIC values across a "window" of related rows **without collapsing** the result (unlike GROUP BY).
# MAGIC
# MAGIC Common window functions:
# MAGIC
# MAGIC | Function | What it does | Example |
# MAGIC |----------|-------------|---------|
# MAGIC | `RANK()` | Rank rows within a group | Top 5 customers per segment |
# MAGIC | `ROW_NUMBER()` | Unique sequential number | Deduplicate records |
# MAGIC | `SUM() OVER` | Running total | Cumulative revenue by month |
# MAGIC | `LAG() / LEAD()` | Previous/next row value | Day-over-day change |
# MAGIC | `AVG() OVER` | Moving average | 7-day rolling average |
# MAGIC
# MAGIC ```sql
# MAGIC -- Syntax pattern:
# MAGIC FUNCTION() OVER (
# MAGIC     PARTITION BY group_column    -- like GROUP BY but doesn't collapse
# MAGIC     ORDER BY sort_column         -- defines the "window" ordering
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- WINDOW FUNCTION EXAMPLE: Top 5 spenders per market segment
# MAGIC -- RANK() assigns 1, 2, 3... within each segment
# MAGIC
# MAGIC SELECT * FROM (
# MAGIC   SELECT 
# MAGIC     c_name,
# MAGIC     c_mktsegment,
# MAGIC     total_spent,
# MAGIC     RANK() OVER (
# MAGIC         PARTITION BY c_mktsegment 
# MAGIC         ORDER BY total_spent DESC
# MAGIC     ) AS rank_in_segment
# MAGIC   FROM (
# MAGIC     SELECT 
# MAGIC         c.c_name,
# MAGIC         c.c_mktsegment,
# MAGIC         ROUND(SUM(o.o_totalprice), 2) AS total_spent
# MAGIC     FROM samples.tpch.orders o
# MAGIC     JOIN samples.tpch.customer c ON o.o_custkey = c.c_custkey
# MAGIC     GROUP BY c.c_name, c.c_mktsegment
# MAGIC   )
# MAGIC ) WHERE rank_in_segment <= 5
# MAGIC ORDER BY c_mktsegment, rank_in_segment

# COMMAND ----------

# MAGIC %md
# MAGIC #### 👆 How the Window Function Worked
# MAGIC
# MAGIC ```
# MAGIC Step 1: Inner query computes total_spent per customer (GROUP BY)
# MAGIC Step 2: RANK() OVER (PARTITION BY segment ORDER BY spent DESC)
# MAGIC         → Within each segment, assigns rank 1 to highest spender
# MAGIC Step 3: Outer query filters to rank <= 5 → Top 5 per segment
# MAGIC ```
# MAGIC
# MAGIC **Key difference from GROUP BY**: GROUP BY collapses rows (750K customers → 5 segments).
# MAGIC Window functions **keep all rows** but add a computed column (rank, running total, etc.)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Another window example: Running total of order value by date
# MAGIC -- Shows cumulative revenue over time for the BUILDING segment
# MAGIC
# MAGIC SELECT 
# MAGIC     o_orderdate,
# MAGIC     daily_revenue,
# MAGIC     SUM(daily_revenue) OVER (ORDER BY o_orderdate) AS cumulative_revenue,
# MAGIC     COUNT(*) OVER (ORDER BY o_orderdate) AS cumulative_days
# MAGIC FROM (
# MAGIC     SELECT 
# MAGIC         o.o_orderdate,
# MAGIC         ROUND(SUM(o.o_totalprice), 2) AS daily_revenue
# MAGIC     FROM samples.tpch.orders o
# MAGIC     JOIN samples.tpch.customer c ON o.o_custkey = c.c_custkey
# MAGIC     WHERE c.c_mktsegment = 'BUILDING'
# MAGIC     GROUP BY o.o_orderdate
# MAGIC )
# MAGIC ORDER BY o_orderdate
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: CREATE TABLE AS SELECT (CTAS)
# MAGIC
# MAGIC You can **materialize** the results of any SQL query as a permanent table.
# MAGIC This is how you build derived datasets, summary tables, and data marts.
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE TABLE catalog.schema.my_summary AS
# MAGIC SELECT ... FROM ... GROUP BY ...
# MAGIC ```
# MAGIC
# MAGIC The result is a **Delta Lake table** (the default) — with full ACID transactions,
# MAGIC schema enforcement, and time travel. You can then query it from Python or SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CTAS example: Create a summary table of revenue by segment and year
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE workspace.default.segment_summary AS
# MAGIC SELECT 
# MAGIC     c.c_mktsegment AS segment,
# MAGIC     YEAR(o.o_orderdate) AS order_year,
# MAGIC     COUNT(*) AS num_orders,
# MAGIC     COUNT(DISTINCT o.o_custkey) AS unique_customers,
# MAGIC     ROUND(SUM(o.o_totalprice), 2) AS total_revenue,
# MAGIC     ROUND(AVG(o.o_totalprice), 2) AS avg_order_value
# MAGIC FROM samples.tpch.orders o
# MAGIC JOIN samples.tpch.customer c ON o.o_custkey = c.c_custkey
# MAGIC GROUP BY c.c_mktsegment, YEAR(o.o_orderdate)
# MAGIC ORDER BY segment, order_year

# COMMAND ----------

# ─── VERIFY: Query our new table from Python ─────────────────────
# The CTAS created a Delta table — accessible from both SQL and Python

summary = spark.read.table("workspace.default.segment_summary")
print("QUERYING THE TABLE WE CREATED WITH SQL")
print("=" * 60)
print(f"  Table: workspace.default.segment_summary")
print(f"  Rows: {summary.count()}")
print(f"  Columns: {summary.columns}")
print()
summary.show(10)

print("KEY TAKEAWAYS:")
print("  1. Created with pure SQL (CREATE TABLE AS SELECT)")
print("  2. Stored as Delta Lake (the default format)")
print("  3. Queryable from Python: spark.read.table('workspace.default.segment_summary')")
print("  4. Queryable from SQL:    SELECT * FROM workspace.default.segment_summary")
print("  5. Has all Delta features: time travel, ACID, schema enforcement")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: SQL vs DataFrame API — Comparison
# MAGIC
# MAGIC The same query written both ways. **Choose whichever is more readable for the task.**
# MAGIC
# MAGIC ### Example: "Top 3 nations by supplier count"

# COMMAND ----------

# ─── SAME QUERY — TWO WAYS ──────────────────────────────────────
from pyspark.sql.functions import count, avg, round as spark_round, desc

supplier = spark.read.table("samples.tpch.supplier")
nation = spark.read.table("samples.tpch.nation")

# Method 1: DataFrame API
print("METHOD 1 — DataFrame API:")
print("-" * 40)
df_result = (supplier
    .join(nation, supplier.s_nationkey == nation.n_nationkey)
    .groupBy("n_name")
    .agg(
        count("s_suppkey").alias("num_suppliers"),
        spark_round(avg("s_acctbal"), 2).alias("avg_balance")
    )
    .orderBy(desc("num_suppliers"))
    .limit(3)
)
df_result.show()

# Method 2: SQL
print("METHOD 2 — SQL:")
print("-" * 40)
sql_result = spark.sql(
    "SELECT n.n_name, COUNT(s.s_suppkey) AS num_suppliers, "
    "ROUND(AVG(s.s_acctbal), 2) AS avg_balance "
    "FROM samples.tpch.supplier s "
    "JOIN samples.tpch.nation n ON s.s_nationkey = n.n_nationkey "
    "GROUP BY n.n_name ORDER BY num_suppliers DESC LIMIT 3"
)
sql_result.show()

print("Both produce IDENTICAL results and execution plans!")
print("Use whichever is more natural for the task at hand.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Now You Try!
# MAGIC
# MAGIC ### Exercise 1: SQL Exploration
# MAGIC Write a `%sql` query to find:
# MAGIC - The **10 most expensive orders** from `samples.tpch.orders`
# MAGIC - Include: order key, customer key, total price, order date, status
# MAGIC - Order by total price descending
# MAGIC
# MAGIC ### Exercise 2: spark.sql() + DataFrame
# MAGIC Using `spark.sql()`:
# MAGIC - Find the average order value per priority level from `samples.tpch.orders`
# MAGIC - Capture the result as a DataFrame
# MAGIC - Use `.filter()` to keep only priorities with avg value above 150,000
# MAGIC - Display the result
# MAGIC
# MAGIC ### Exercise 3: CTE Challenge
# MAGIC Write a CTE query that:
# MAGIC - CTE 1: Compute each nation's total supplier revenue (sum of `s_acctbal` from `samples.tpch.supplier`)
# MAGIC - CTE 2: Find the top region for each nation (join with `samples.tpch.region`)
# MAGIC - Final: Show the top 5 nations by supplier revenue, with their region name
# MAGIC
# MAGIC ### Exercise 4: Window Function
# MAGIC Using a window function:
# MAGIC - Rank all order priorities within each year
# MAGIC - Show for each priority: year, priority, order count, rank within that year
# MAGIC - Only show the top 3 priorities per year

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EXERCISE 1: Your SQL here

# COMMAND ----------

# EXERCISE 2: Your Python + SQL here
# result = spark.sql("...")
# result.filter(...).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EXERCISE 3: Your CTE here

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EXERCISE 4: Your window function here

# COMMAND ----------

# ─── CLEANUP ──────────────────────────────────────────────────────
print("Cleaning up...")
spark.sql("DROP TABLE IF EXISTS workspace.default.segment_summary")
print("  Done! SQL notebook complete.")
