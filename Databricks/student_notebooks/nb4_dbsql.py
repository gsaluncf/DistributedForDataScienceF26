# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 4: DBSQL & the Modern Databricks Stack
# MAGIC 
# MAGIC **Course**: Distributed Systems for Data Science  
# MAGIC **Week**: 7 — Spark & the Lakehouse
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## From PySpark to SQL-First Workflows
# MAGIC
# MAGIC In Notebooks 1-3, you worked directly with PySpark DataFrames — writing `.filter()`,
# MAGIC `.groupBy()`, `.join()`, and reading execution plans. You saw the distributed system up close.
# MAGIC
# MAGIC Now we move up one abstraction layer. **Databricks SQL (DBSQL)** lets you write standard SQL
# MAGIC and the platform handles all the distributed details: partitioning, shuffles, parallelism,
# MAGIC and optimization. The same Spark engine runs underneath — you just interact with it differently.
# MAGIC
# MAGIC ### Why This Matters
# MAGIC
# MAGIC Quan, who leads Databricks Public Sector, observes:  
# MAGIC *"Among customers, Spark is mentioned less frequently. DBSQL is experiencing rapid growth.
# MAGIC DBSQL simplifies workflows by abstracting many Spark details."*
# MAGIC
# MAGIC This notebook shows you both sides so you can see what you gain and what you trade away.
# MAGIC
# MAGIC > [Spark SQL Reference](https://spark.apache.org/docs/latest/sql-ref.html) | [Databricks SQL Guide](https://docs.databricks.com/aws/en/sql/) | [Query Profile](https://docs.databricks.com/aws/en/compute/sql-warehouse/query-profile.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: MPP — How Your Query Actually Runs
# MAGIC
# MAGIC MPP (Massively Parallel Processing) is the architecture behind both DBSQL and PySpark:
# MAGIC
# MAGIC 1. A **coordinator node** receives your query
# MAGIC 2. It creates an **execution plan** that breaks the query into tasks
# MAGIC 3. **Worker nodes** each execute a portion of the tasks in parallel
# MAGIC 4. Results are **aggregated** back at the coordinator
# MAGIC
# MAGIC This is the same distributed pattern from earlier in the course: decompose work,
# MAGIC distribute it, combine results. The difference is that **you never see the parallelism**.
# MAGIC
# MAGIC ```
# MAGIC You write:        SELECT AVG(salary) FROM employees WHERE dept = 'Engineering'
# MAGIC
# MAGIC What happens:     Worker 1: scan partition A → filter → partial sum + count
# MAGIC                   Worker 2: scan partition B → filter → partial sum + count
# MAGIC                   Worker 3: scan partition C → filter → partial sum + count
# MAGIC                   Coordinator: combine partials → final AVG
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Sample Data
# MAGIC
# MAGIC We will create a sample dataset to use throughout this notebook.
# MAGIC The `samples.tpch` tables are read-only, so we build our own writable table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a 10,000-row employees table in our writable catalog
# MAGIC CREATE OR REPLACE TABLE workspace.default.nb4_employees AS
# MAGIC SELECT
# MAGIC   id,
# MAGIC   CASE WHEN id % 5 = 0 THEN 'Engineering'
# MAGIC        WHEN id % 5 = 1 THEN 'Marketing'
# MAGIC        WHEN id % 5 = 2 THEN 'Sales'
# MAGIC        WHEN id % 5 = 3 THEN 'Operations'
# MAGIC        ELSE 'Finance'
# MAGIC   END AS department,
# MAGIC   ROUND(40000 + RAND() * 80000, 2) AS salary,
# MAGIC   DATE_ADD('2015-01-01', CAST(RAND() * 3650 AS INT)) AS hire_date,
# MAGIC   CASE WHEN RAND() > 0.3 THEN 'Active' ELSE 'Inactive' END AS status
# MAGIC FROM RANGE(10000) AS t(id);
# MAGIC 
# MAGIC SELECT COUNT(*) AS total_employees, COUNT(DISTINCT department) AS departments FROM workspace.default.nb4_employees

# COMMAND ----------

# MAGIC %md
# MAGIC You created a Delta Lake table with 10,000 rows and 5 departments.
# MAGIC Even at this scale, DBSQL distributes the work across available compute.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Section 2: DBSQL vs PySpark — Same Operation, Two Abstractions
# MAGIC
# MAGIC The same aggregation written in SQL and PySpark. Both compile to the same execution plan.
# MAGIC
# MAGIC ### The SQL Way

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Average salary by department for active employees
# MAGIC SELECT
# MAGIC   department,
# MAGIC   COUNT(*) AS headcount,
# MAGIC   ROUND(AVG(salary), 2) AS avg_salary,
# MAGIC   ROUND(MIN(salary), 2) AS min_salary,
# MAGIC   ROUND(MAX(salary), 2) AS max_salary
# MAGIC FROM workspace.default.nb4_employees
# MAGIC WHERE status = 'Active'
# MAGIC GROUP BY department
# MAGIC ORDER BY avg_salary DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### The PySpark Way
# MAGIC
# MAGIC Exact same operation, different syntax:

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table("workspace.default.nb4_employees")

result = (
    df
    .filter(F.col("status") == "Active")
    .groupBy("department")
    .agg(
        F.count("*").alias("headcount"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.round(F.min("salary"), 2).alias("min_salary"),
        F.round(F.max("salary"), 2).alias("max_salary")
    )
    .orderBy(F.desc("avg_salary"))
)

result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### What to Notice
# MAGIC
# MAGIC | Aspect | DBSQL | PySpark |
# MAGIC |--------|-------|---------|
# MAGIC | Lines of code | 8 | 14 |
# MAGIC | Concepts required | SQL (widely known) | Spark API, DataFrames, Column objects |
# MAGIC | Distributed details visible? | No | Partially (`.groupBy()`, `.agg()`) |
# MAGIC | Who can read this? | Any analyst | Only Spark developers |
# MAGIC
# MAGIC Both produce **identical** results and execution plans. Click **"See performance"** on both
# MAGIC cells to confirm this — the DAGs will look the same.
# MAGIC
# MAGIC This is why DBSQL is growing: it lowers the barrier while keeping the same distributed engine underneath.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.1: Write It Both Ways
# MAGIC
# MAGIC For each **year** of hiring (from `hire_date`), find:
# MAGIC - Number of employees hired
# MAGIC - Average starting salary
# MAGIC
# MAGIC Write it first in SQL, then in PySpark. Compare the experience.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EXERCISE 2.1a: SQL version
# MAGIC -- Hint: Use YEAR(hire_date) to extract the year
# MAGIC -- Expected columns: hire_year, num_hired, avg_salary
# MAGIC 
# MAGIC -- YOUR QUERY HERE

# COMMAND ----------

# EXERCISE 2.1b: PySpark version
# Hint: Use F.year("hire_date") to extract the year
# Expected columns: hire_year, num_hired, avg_salary

# YOUR CODE HERE

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 3: Unity Catalog in Action
# MAGIC
# MAGIC You already learned about Unity Catalog's three-level namespace in the guide
# MAGIC (`catalog.schema.table`). Let us explore it live.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What catalogs can you see?
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What schemas are in the workspace catalog?
# MAGIC SHOW SCHEMAS IN workspace

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What tables exist in workspace.default?
# MAGIC SHOW TABLES IN workspace.default

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect the employees table we created
# MAGIC DESCRIBE EXTENDED workspace.default.nb4_employees

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.1: Catalog Navigation
# MAGIC
# MAGIC Using the **Catalog sidebar** (left panel in Databricks) and the SQL commands above, answer:
# MAGIC
# MAGIC 1. What catalog is the `nb4_employees` table in?
# MAGIC 2. What schema?
# MAGIC 3. What is the fully qualified name?
# MAGIC 4. What format is it stored in? (Check DESCRIBE EXTENDED output)
# MAGIC 5. How is catalog.schema.table similar to a file path like `/users/me/documents/file.txt`?

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 4: Serverless Compute — The Platform Manages the Cluster
# MAGIC
# MAGIC In a traditional setup, you would:
# MAGIC 1. Choose the number of workers and instance type
# MAGIC 2. Configure auto-scaling
# MAGIC 3. Wait 3-5 minutes for the cluster to start
# MAGIC 4. Run your workload
# MAGIC 5. Remember to shut it down
# MAGIC
# MAGIC With **serverless compute** (what you are using in Free Edition), you skip all of that.
# MAGIC The platform provisions compute automatically.
# MAGIC
# MAGIC **Tradeoff**: You gain simplicity and fast startup. You lose fine-grained control over
# MAGIC parallelism and cost optimization. As Quan noted, job clusters still exist for when that control matters.
# MAGIC
# MAGIC ### Observe Distributed Execution with the Query Profile
# MAGIC
# MAGIC Run the query below, then click **"See performance"** to see the distributed execution plan:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Multi-stage query: filter, aggregate, join, re-aggregate
# MAGIC -- This forces multiple shuffle operations
# MAGIC 
# MAGIC WITH dept_stats AS (
# MAGIC   SELECT
# MAGIC     department,
# MAGIC     AVG(salary) AS dept_avg_salary
# MAGIC   FROM workspace.default.nb4_employees
# MAGIC   WHERE status = 'Active'
# MAGIC   GROUP BY department
# MAGIC ),
# MAGIC above_avg AS (
# MAGIC   SELECT
# MAGIC     e.id,
# MAGIC     e.department,
# MAGIC     e.salary,
# MAGIC     d.dept_avg_salary,
# MAGIC     ROUND(e.salary - d.dept_avg_salary, 2) AS above_by
# MAGIC   FROM workspace.default.nb4_employees e
# MAGIC   JOIN dept_stats d ON e.department = d.department
# MAGIC   WHERE e.salary > d.dept_avg_salary
# MAGIC     AND e.status = 'Active'
# MAGIC )
# MAGIC SELECT
# MAGIC   department,
# MAGIC   COUNT(*) AS num_above_avg,
# MAGIC   ROUND(AVG(above_by), 2) AS avg_amount_above
# MAGIC FROM above_avg
# MAGIC GROUP BY department
# MAGIC ORDER BY num_above_avg DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### What to Look for in the Query Profile
# MAGIC
# MAGIC After clicking **"See performance"**, look for:
# MAGIC
# MAGIC 1. **Scan nodes** — where table data is read (happens in parallel across workers)
# MAGIC 2. **Filter nodes** — `WHERE` conditions applied (pushed down to each worker)
# MAGIC 3. **Exchange/Shuffle nodes** — data redistributed across workers (**the expensive part**)
# MAGIC 4. **Aggregate nodes** — `GROUP BY` computations
# MAGIC 5. **Join nodes** — CTE result joined back to the original table
# MAGIC
# MAGIC The **exchanges are the most interesting** from a distributed systems perspective.
# MAGIC They represent data movement across the network — the same shuffles you saw in
# MAGIC `df.explain(True)` output in Notebook 1.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 5: Hands-On Exercises
# MAGIC
# MAGIC ### Exercise 5.1: Window Functions on Distributed Data
# MAGIC
# MAGIC Rank employees within each department by salary (highest first).
# MAGIC Show only the **top 3 per department** for active employees.
# MAGIC
# MAGIC Include: `department`, `id`, `salary`, `rank_in_dept`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EXERCISE 5.1: Window function
# MAGIC -- Hint: RANK() OVER (PARTITION BY department ORDER BY salary DESC)
# MAGIC -- Then filter for rank <= 3
# MAGIC 
# MAGIC -- YOUR QUERY HERE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.2: Partitioning and Scan Efficiency
# MAGIC
# MAGIC Create a partitioned version of the employees table, then compare Query Profiles.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a partitioned version
# MAGIC CREATE OR REPLACE TABLE workspace.default.nb4_employees_partitioned
# MAGIC PARTITIONED BY (department)
# MAGIC AS SELECT * FROM workspace.default.nb4_employees;
# MAGIC 
# MAGIC -- Query WITH partition pruning (only reads Engineering partition)
# MAGIC SELECT COUNT(*), ROUND(AVG(salary), 2) AS avg_salary
# MAGIC FROM workspace.default.nb4_employees_partitioned
# MAGIC WHERE department = 'Engineering'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Same query on the NON-partitioned table for comparison
# MAGIC -- Compare "See performance" for both: notice the Scan node differences
# MAGIC SELECT COUNT(*), ROUND(AVG(salary), 2) AS avg_salary
# MAGIC FROM workspace.default.nb4_employees
# MAGIC WHERE department = 'Engineering'

# COMMAND ----------

# MAGIC %md
# MAGIC **Question**: Compare the Query Profile for both cells above.
# MAGIC How many bytes did each Scan node read? What does this tell you about how partitioning
# MAGIC helps distributed queries skip irrelevant data?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.3: Reflection Questions
# MAGIC
# MAGIC Create a new cell below (or write on paper) and answer:
# MAGIC
# MAGIC 1. In the MPP model, what role does the coordinator play versus the workers?
# MAGIC    How is this similar to other distributed systems we studied (Lambda/SQS, P2P gossip)?
# MAGIC
# MAGIC 2. When you wrote the same aggregation in SQL and PySpark, the execution plans were nearly
# MAGIC    identical. If the underlying engine is the same, what concrete advantages does
# MAGIC    the SQL interface provide? What can you do in PySpark that SQL cannot express?
# MAGIC
# MAGIC 3. The Query Profile showed exchange/shuffle operations. Why are these the most expensive
# MAGIC    part of distributed execution? What network and coordination costs are involved?
# MAGIC
# MAGIC 4. Quan observed a large customer migrating from EMR (self-managed Spark) to DBSQL.
# MAGIC    From a distributed systems perspective, what risks does an organization take on
# MAGIC    when it hands cluster control to a managed platform?

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 6: The Evolving Ecosystem
# MAGIC
# MAGIC Beyond DBSQL, Unity Catalog, and serverless compute, the Databricks platform keeps growing.
# MAGIC Here are areas Quan highlighted as important trends:
# MAGIC
# MAGIC | Feature | What It Does | Why It Matters |
# MAGIC |---------|-------------|----------------|
# MAGIC | **Lakeflow SDP** | Spark Declarative Pipelines — define ETL as declarations, not code | Moves from "how" to "what" |
# MAGIC | **Genie** | Natural language to SQL | Non-technical users query data by asking questions in English |
# MAGIC | **Databricks Assistant** | AI code generation within notebooks | Generates PySpark, SQL, or Python from descriptions |
# MAGIC | **Databricks Apps** | Build interactive apps in the workspace | Data platforms accessible to non-engineers |
# MAGIC | **AI Agents** | Build agents that reason over your data | Systems that query data *and* take actions |
# MAGIC
# MAGIC ### The Pattern
# MAGIC
# MAGIC Notice: each raises the abstraction layer.
# MAGIC Spark abstracted Hadoop. DBSQL abstracted Spark. Genie abstracts SQL. AI agents may abstract the entire workflow.
# MAGIC
# MAGIC From a distributed systems perspective, the distributed computing is not going away — it is being
# MAGIC hidden behind progressively higher abstractions. Understanding what happens underneath (partitioning,
# MAGIC shuffles, coordination, fault tolerance) is what separates someone who can *use* these tools
# MAGIC from someone who can *debug and optimize* them when things go wrong.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Summary
# MAGIC
# MAGIC **What we covered:**
# MAGIC
# MAGIC 1. **MPP Architecture** — coordinator distributes work to workers, workers process in parallel, results are combined
# MAGIC 2. **DBSQL vs PySpark** — same engine, different interfaces. SQL is simpler and more accessible. PySpark gives more control.
# MAGIC 3. **Unity Catalog** — three-level namespace for data governance. Solves the discovery problem at platform level.
# MAGIC 4. **Serverless Compute** — platform manages cluster lifecycle. You gain simplicity, you lose fine-grained control.
# MAGIC 5. **Query Profiles** — visual tool to observe how SQL is decomposed into distributed tasks and shuffles.
# MAGIC 6. **The Ecosystem** — Lakeflow, Genie, Databricks Apps, AI Agents — each raises the abstraction layer.
# MAGIC
# MAGIC **Key takeaway**: The distributed computing fundamentals you learned with PySpark have not changed.
# MAGIC What has changed is how much of it the platform manages for you. Understanding both levels —
# MAGIC the abstraction and the mechanism — is what makes you effective.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next**: Return to the guide for Part 7 (History and Context).

# COMMAND ----------

# --- CLEANUP -----------------------------------------------------------------
print("Cleaning up...")
spark.sql("DROP TABLE IF EXISTS workspace.default.nb4_employees")
spark.sql("DROP TABLE IF EXISTS workspace.default.nb4_employees_partitioned")
print("  Dropped nb4_employees and nb4_employees_partitioned")
print("  Notebook 4 complete!")
