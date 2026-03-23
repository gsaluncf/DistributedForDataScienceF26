# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 7: dbt Core — SQL-First Data Transformation
# MAGIC
# MAGIC **Course**: Distributed Systems for Data Science  
# MAGIC **Week**: 9, Data Transformation Pipelines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC
# MAGIC This notebook introduces **dbt Core** — a free, open-source command-line tool
# MAGIC that transforms data in your warehouse using plain SQL. You will:
# MAGIC
# MAGIC 1. Understand **what dbt is** and why the industry adopted it
# MAGIC 2. See how a dbt project is structured (models, sources, tests)
# MAGIC 3. Build the **same custodian pipeline** from notebooks 5 and 6 — this time as dbt models
# MAGIC 4. Run `dbt run` and `dbt test` against your Databricks warehouse
# MAGIC 5. Compare dbt with DLT and other approaches
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC - You have completed notebooks 1–6 (especially nb5 and nb6)
# MAGIC - You have a Databricks SQL Warehouse available (the Starter Warehouse works)
# MAGIC - The Bronze tables from nb5a still exist: `workspace.pipeline.bronze_holdings`
# MAGIC   and `workspace.pipeline.bronze_prices`
# MAGIC
# MAGIC > **Reading**: [dbt Core Guide](dbt.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: What Is dbt?
# MAGIC
# MAGIC ### The Problem
# MAGIC
# MAGIC In notebooks 5a-5c, you built a Medallion pipeline by writing PySpark code in
# MAGIC three notebooks, wiring them together in a Workflow, and manually handling
# MAGIC reads/writes. In notebook 6, you used DLT (Databricks' declarative approach) to
# MAGIC let the platform handle orchestration.
# MAGIC
# MAGIC But what if you want something that:
# MAGIC - Works across **any** data warehouse (Snowflake, BigQuery, Redshift, Databricks)
# MAGIC - Uses **plain SQL** — no PySpark, no proprietary decorators
# MAGIC - Brings **software engineering practices** to data: version control, testing, docs, CI/CD
# MAGIC - Is **free and open source**
# MAGIC
# MAGIC That's **dbt Core**.
# MAGIC
# MAGIC ### The "T" in ELT
# MAGIC
# MAGIC ```
# MAGIC ┌─────────┐    ┌─────────┐    ┌──────────────┐
# MAGIC │ Extract │ →  │  Load   │ →  │  Transform   │  ← dbt lives here
# MAGIC │ (Fivetran,│   │ (into   │    │ (SQL models  │
# MAGIC │  Airbyte) │   │  warehouse)│  │  in the      │
# MAGIC └─────────┘    └─────────┘    │  warehouse)  │
# MAGIC                               └──────────────┘
# MAGIC ```
# MAGIC
# MAGIC dbt does **not** extract or load data. It assumes your raw data is already in the
# MAGIC warehouse (our Bronze tables). dbt handles only the **Transform** step — turning
# MAGIC raw data into clean, tested, documented tables using SQL `SELECT` statements.
# MAGIC
# MAGIC ### How It Works (30-Second Version)
# MAGIC
# MAGIC 1. You write SQL `SELECT` statements in `.sql` files called **models**
# MAGIC 2. Each model becomes a table or view in your warehouse
# MAGIC 3. Models reference each other with `{{ ref('model_name') }}` — dbt builds the DAG
# MAGIC 4. You define **tests** in YAML: `unique`, `not_null`, `accepted_values`
# MAGIC 5. You run `dbt run` to execute all models in dependency order
# MAGIC 6. You run `dbt test` to validate data quality
# MAGIC
# MAGIC > **Key insight**: dbt models are just SQL `SELECT` statements. dbt wraps them in
# MAGIC > `CREATE TABLE AS SELECT` or `CREATE VIEW AS` for you. You never write DDL.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Install dbt Core + Databricks Adapter
# MAGIC
# MAGIC We install `dbt-databricks`, which automatically installs `dbt-core` as a dependency.
# MAGIC
# MAGIC ⚠️ **This cell restarts the Python environment.** It must be the first code cell.

# COMMAND ----------

# MAGIC %pip install dbt-databricks --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Set Up a dbt Project
# MAGIC
# MAGIC A dbt project is just a folder with a specific structure. In production, this
# MAGIC lives in a Git repo. Here we'll create a mini project on the cluster's filesystem
# MAGIC to demonstrate the concepts.
# MAGIC
# MAGIC ### Project Structure
# MAGIC ```
# MAGIC custodian_dbt/
# MAGIC ├── dbt_project.yml          # Project config: name, version, materialization defaults
# MAGIC ├── profiles.yml             # Connection config: how to talk to Databricks
# MAGIC ├── models/
# MAGIC │   ├── sources.yml          # Declare raw tables (our Bronze layer)
# MAGIC │   ├── staging/
# MAGIC │   │   ├── stg_holdings.sql # Staging model: clean raw holdings
# MAGIC │   │   └── stg_prices.sql   # Staging model: clean raw prices
# MAGIC │   ├── marts/
# MAGIC │   │   ├── portfolio_valuation.sql  # Mart model: join + compute
# MAGIC │   │   └── sector_allocation.sql    # Mart model: aggregate
# MAGIC │   └── schema.yml           # Tests and documentation for models
# MAGIC ```
# MAGIC
# MAGIC Every file has a clear role. Let's build each one.

# COMMAND ----------

import os, json, textwrap
from pathlib import Path

# ── Project root on the cluster filesystem ───────────────────────
PROJECT_DIR = "/tmp/custodian_dbt"
os.makedirs(f"{PROJECT_DIR}/models/staging", exist_ok=True)
os.makedirs(f"{PROJECT_DIR}/models/marts", exist_ok=True)

print(f"dbt project directory: {PROJECT_DIR}")
print("Directory structure created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 `dbt_project.yml` — The Project Config
# MAGIC
# MAGIC This file tells dbt what the project is called and how to materialize models.
# MAGIC "Materialize" means: should the SQL result become a **table** or a **view**?
# MAGIC
# MAGIC - **view** (default): `CREATE VIEW AS SELECT ...` — lightweight, always fresh, but slow to query
# MAGIC - **table**: `CREATE TABLE AS SELECT ...` — stored on disk, fast to query, stale until re-run
# MAGIC - **incremental**: append new rows only — best for large event tables

# COMMAND ----------

# ── dbt_project.yml ──────────────────────────────────────────────
dbt_project_yml = textwrap.dedent("""\
    name: 'custodian_pipeline'
    version: '1.0.0'

    profile: 'databricks_demo'

    model-paths: ["models"]
    test-paths: ["tests"]

    target-path: "target"
    clean-targets: ["target", "dbt_packages"]

    models:
      custodian_pipeline:
        staging:
          +materialized: view        # Staging models are views (lightweight)
        marts:
          +materialized: table       # Mart models are tables (fast to query)
""")

Path(f"{PROJECT_DIR}/dbt_project.yml").write_text(dbt_project_yml)
print("Created: dbt_project.yml")
print()
print(dbt_project_yml)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 `profiles.yml` — The Connection Config
# MAGIC
# MAGIC This file tells dbt **how to connect** to your Databricks SQL Warehouse.
# MAGIC In production, this lives in `~/.dbt/profiles.yml` (not in the project repo,
# MAGIC because it contains credentials). Here we generate it dynamically.
# MAGIC
# MAGIC The key fields:
# MAGIC - **host**: Your Databricks workspace URL
# MAGIC - **http_path**: The SQL Warehouse endpoint path
# MAGIC - **token**: Your personal access token
# MAGIC - **catalog / schema**: Where dbt should create tables

# COMMAND ----------

# ── Build profiles.yml from the current Databricks context ───────
# When running inside Databricks, we can get connection info from the context

# Get the workspace host from the Spark config
host = spark.conf.get("spark.databricks.workspaceUrl")
# Get a token for the current session
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
# Get the SQL Warehouse HTTP path
warehouses = spark.conf.get("spark.databricks.sql.warehouseId", "")

# We'll use the serverless SQL warehouse path format
# For the starter warehouse, we need to get the http_path differently
import requests
api_url = f"https://{host}/api/2.0/sql/warehouses"
headers = {"Authorization": f"Bearer {token}"}
resp = requests.get(api_url, headers=headers)
wh_list = resp.json().get("warehouses", [])

if wh_list:
    wh = wh_list[0]  # Use the first available warehouse
    http_path = f"/sql/1.0/warehouses/{wh['id']}"
    print(f"Using SQL Warehouse: {wh['name']} (state: {wh.get('state', '?')})")
    print(f"HTTP path: {http_path}")
else:
    # Fallback: use serverless compute
    http_path = "/sql/1.0/warehouses/serverless"
    print("No SQL warehouse found, using serverless compute path")

# COMMAND ----------

# ── Write profiles.yml ───────────────────────────────────────────
profiles_yml = textwrap.dedent(f"""\
    databricks_demo:
      target: dev
      outputs:
        dev:
          type: databricks
          host: {host}
          http_path: {http_path}
          token: {token}
          catalog: workspace
          schema: dbt_demo
          threads: 4
""")

Path(f"{PROJECT_DIR}/profiles.yml").write_text(profiles_yml)
print("Created: profiles.yml")
print()
# Print with token masked
print(profiles_yml.replace(token, "dapi***REDACTED***"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 `sources.yml` — Declare Your Raw Data
# MAGIC
# MAGIC In dbt, **sources** are the raw tables that already exist in your warehouse.
# MAGIC You don't create them with dbt — they were loaded by some other process
# MAGIC (our Bronze layer notebooks, or an EL tool like Fivetran).
# MAGIC
# MAGIC Declaring sources gives you:
# MAGIC 1. **Lineage tracking**: dbt knows where your data comes from
# MAGIC 2. **Freshness checks**: dbt can alert you if source data is stale
# MAGIC 3. **The `source()` function**: Reference raw tables by name instead of hardcoding paths
# MAGIC
# MAGIC ```sql
# MAGIC -- Instead of this (fragile, hardcoded):
# MAGIC SELECT * FROM workspace.pipeline.bronze_holdings
# MAGIC
# MAGIC -- You write this (portable, tracked):
# MAGIC SELECT * FROM {{ source('pipeline', 'bronze_holdings') }}
# MAGIC ```

# COMMAND ----------

# ── sources.yml ──────────────────────────────────────────────────
sources_yml = textwrap.dedent("""\
    version: 2

    sources:
      - name: pipeline            # Logical name for this source group
        database: workspace       # The Unity Catalog name
        schema: pipeline          # The schema where Bronze tables live
        description: "Raw Bronze tables from the custodian report ingestion (nb5a)"

        tables:
          - name: bronze_holdings
            description: "Raw custodian holdings with messy tickers, dollar signs, blanks, and duplicates"
            columns:
              - name: Ticker
                description: "Stock ticker symbol (may include exchange prefix like NYSE:MSFT)"
              - name: Shares_Held
                description: "Number of shares (string, may contain non-numeric characters)"
              - name: Cost_Basis
                description: "Purchase price per share (string, may contain $ signs)"

          - name: bronze_prices
            description: "Raw end-of-day prices with mixed date formats and bad records"
            columns:
              - name: Symbol
                description: "Stock ticker symbol"
              - name: Close
                description: "Closing price (string, may contain $ or N/A)"
              - name: Date
                description: "Price date (mixed formats: yyyy-MM-dd or MM/DD/YYYY)"
""")

Path(f"{PROJECT_DIR}/models/sources.yml").write_text(sources_yml)
print("Created: models/sources.yml")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Write the Models
# MAGIC
# MAGIC Each dbt model is a `.sql` file containing a single `SELECT` statement.
# MAGIC That's it. No `CREATE TABLE`, no `INSERT INTO`, no `.write.saveAsTable()`.
# MAGIC
# MAGIC dbt wraps your `SELECT` in the appropriate DDL based on the materialization
# MAGIC setting in `dbt_project.yml`.
# MAGIC
# MAGIC ### The `ref()` and `source()` Functions
# MAGIC
# MAGIC These are dbt's dependency-declaration mechanism (same idea as `dlt.read()`):
# MAGIC
# MAGIC | Function | Purpose | Example |
# MAGIC |----------|---------|---------|
# MAGIC | `{{ source('group', 'table') }}` | Reference a raw source table | `{{ source('pipeline', 'bronze_holdings') }}` |
# MAGIC | `{{ ref('model_name') }}` | Reference another dbt model | `{{ ref('stg_holdings') }}` |
# MAGIC
# MAGIC When dbt sees `ref('stg_holdings')` inside `portfolio_valuation.sql`, it knows
# MAGIC `portfolio_valuation` depends on `stg_holdings` and must run after it. **This is
# MAGIC how dbt builds its DAG** — the same concept as `dlt.read()` in notebook 6.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Staging Models (Bronze → Silver)
# MAGIC
# MAGIC Staging models clean and normalize raw source data. They are the dbt equivalent
# MAGIC of the Silver layer. We materialize them as **views** because they are
# MAGIC intermediate — downstream models will query them, but end users should not.

# COMMAND ----------

# ── models/staging/stg_holdings.sql ──────────────────────────────
stg_holdings_sql = textwrap.dedent("""\
    -- stg_holdings: Clean the raw custodian holdings
    -- Equivalent to the Silver Holdings cleaning in nb5b

    WITH source AS (
        SELECT * FROM {{ source('pipeline', 'bronze_holdings') }}
    ),

    cleaned AS (
        SELECT
            -- Strip whitespace and exchange prefixes (NYSE:MSFT → MSFT)
            TRIM(REGEXP_REPLACE(Ticker, '^[A-Z]+:', '')) AS ticker,
            Company_Name AS company_name,

            -- Remove non-numeric characters from shares, cast to int
            CAST(REGEXP_REPLACE(Shares_Held, '[^0-9]', '') AS INT) AS shares_held,

            -- Remove dollar signs and commas, cast to double
            CAST(REGEXP_REPLACE(Cost_Basis, '[\\\\$,]', '') AS DOUBLE) AS cost_basis,

            Sector AS sector,
            Account AS account

        FROM source
        WHERE Ticker IS NOT NULL   -- Drop blank rows
    ),

    deduplicated AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ticker) AS _row_num
        FROM cleaned
    )

    SELECT
        ticker, company_name, shares_held, cost_basis, sector, account
    FROM deduplicated
    WHERE _row_num = 1             -- Keep first occurrence only
""")

Path(f"{PROJECT_DIR}/models/staging/stg_holdings.sql").write_text(stg_holdings_sql)
print("Created: models/staging/stg_holdings.sql")
print()
print(stg_holdings_sql)

# COMMAND ----------

# ── models/staging/stg_prices.sql ────────────────────────────────
stg_prices_sql = textwrap.dedent("""\
    -- stg_prices: Clean the raw market prices
    -- Equivalent to the Silver Prices cleaning in nb5b

    WITH source AS (
        SELECT * FROM {{ source('pipeline', 'bronze_prices') }}
    ),

    cleaned AS (
        SELECT
            TRIM(Symbol) AS symbol,

            -- Normalize mixed date formats to proper DATE
            CASE
                WHEN Date RLIKE '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
                THEN TO_DATE(Date, 'MM/dd/yyyy')
                ELSE TO_DATE(Date, 'yyyy-MM-dd')
            END AS price_date,

            -- Remove dollar signs, handle N/A
            CAST(
                NULLIF(REGEXP_REPLACE(Close, '[\\\\$,]', ''), 'N/A')
                AS DOUBLE
            ) AS close_price,

            CAST(REGEXP_REPLACE(Volume, '[^0-9]', '') AS INT) AS volume,

            CAST(
                NULLIF(Change_Pct, '-')
                AS DOUBLE
            ) AS change_pct

        FROM source
        WHERE Symbol IS NOT NULL
    ),

    deduplicated AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY symbol, price_date
                ORDER BY symbol
            ) AS _row_num
        FROM cleaned
        WHERE close_price IS NOT NULL   -- Drop N/A prices
    )

    SELECT
        symbol, price_date, close_price, volume, change_pct
    FROM deduplicated
    WHERE _row_num = 1
""")

Path(f"{PROJECT_DIR}/models/staging/stg_prices.sql").write_text(stg_prices_sql)
print("Created: models/staging/stg_prices.sql")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Mart Models (Silver → Gold)
# MAGIC
# MAGIC Mart models are the final, business-facing tables. They are the dbt equivalent
# MAGIC of the Gold layer. We materialize them as **tables** because they are queried
# MAGIC directly by analysts, dashboards, and BI tools.
# MAGIC
# MAGIC Notice how `ref('stg_holdings')` and `ref('stg_prices')` declare the dependencies.
# MAGIC dbt will run the staging models first, then the mart models.

# COMMAND ----------

# ── models/marts/portfolio_valuation.sql ─────────────────────────
portfolio_sql = textwrap.dedent("""\
    -- portfolio_valuation: Join holdings with prices to compute portfolio metrics
    -- Equivalent to the Gold Portfolio table in nb5c / nb6

    WITH holdings AS (
        SELECT * FROM {{ ref('stg_holdings') }}
    ),

    prices AS (
        SELECT * FROM {{ ref('stg_prices') }}
    ),

    joined AS (
        SELECT
            h.ticker,
            h.company_name,
            h.sector,
            h.shares_held,
            h.cost_basis,
            p.close_price AS current_price,
            p.change_pct AS daily_change_pct,
            p.price_date,

            ROUND(h.shares_held * p.close_price, 2) AS market_value,
            ROUND(h.shares_held * h.cost_basis, 2) AS cost_value,
            ROUND(
                h.shares_held * p.close_price - h.shares_held * h.cost_basis, 2
            ) AS unrealized_pnl

        FROM holdings h
        INNER JOIN prices p ON h.ticker = p.symbol
    )

    SELECT
        *,
        ROUND(
            market_value / SUM(market_value) OVER () * 100, 2
        ) AS weight_pct
    FROM joined
""")

Path(f"{PROJECT_DIR}/models/marts/portfolio_valuation.sql").write_text(portfolio_sql)
print("Created: models/marts/portfolio_valuation.sql")

# COMMAND ----------

# ── models/marts/sector_allocation.sql ───────────────────────────
sector_sql = textwrap.dedent("""\
    -- sector_allocation: Aggregate portfolio by sector
    -- Equivalent to the Gold Sector Allocation table in nb6

    WITH portfolio AS (
        SELECT * FROM {{ ref('portfolio_valuation') }}
    ),

    aggregated AS (
        SELECT
            sector,
            SUM(market_value) AS sector_value,
            SUM(shares_held) AS total_shares,
            ROUND(SUM(unrealized_pnl), 2) AS sector_pnl

        FROM portfolio
        GROUP BY sector
    )

    SELECT
        *,
        ROUND(
            sector_value / SUM(sector_value) OVER () * 100, 2
        ) AS allocation_pct
    FROM aggregated
    ORDER BY sector_value DESC
""")

Path(f"{PROJECT_DIR}/models/marts/sector_allocation.sql").write_text(sector_sql)
print("Created: models/marts/sector_allocation.sql")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 `schema.yml` — Tests and Documentation
# MAGIC
# MAGIC This is where dbt's software engineering DNA shines. Instead of writing
# MAGIC ad-hoc assertions scattered through your code, you declare data quality rules
# MAGIC in a YAML file — **right next to the model definitions**.
# MAGIC
# MAGIC dbt's built-in generic tests:
# MAGIC
# MAGIC | Test | What It Checks | dbt Equivalent of... |
# MAGIC |------|----------------|---------------------|
# MAGIC | `unique` | No duplicate values | `@dlt.expect("unique", ...)` |
# MAGIC | `not_null` | No null values | `@dlt.expect_or_drop("not_null", ...)` |
# MAGIC | `accepted_values` | Only valid values | Custom filter in nb5b |
# MAGIC | `relationships` | Foreign key integrity | Manual join validation |
# MAGIC
# MAGIC You can also write **singular tests** — custom SQL queries in `tests/*.sql`
# MAGIC that should return **zero rows** if the test passes.

# COMMAND ----------

# ── models/schema.yml ────────────────────────────────────────────
schema_yml = textwrap.dedent("""\
    version: 2

    models:
      - name: stg_holdings
        description: "Cleaned and deduplicated custodian holdings"
        columns:
          - name: ticker
            description: "Normalized stock ticker (e.g., AAPL, MSFT)"
            tests:
              - unique
              - not_null
          - name: shares_held
            tests:
              - not_null
          - name: cost_basis
            tests:
              - not_null
          - name: sector
            tests:
              - accepted_values:
                  values: ['Technology', 'Consumer', 'Financials', 'Healthcare']

      - name: stg_prices
        description: "Cleaned and deduplicated market prices"
        columns:
          - name: symbol
            tests:
              - unique
              - not_null
          - name: close_price
            tests:
              - not_null
          - name: price_date
            tests:
              - not_null

      - name: portfolio_valuation
        description: "Portfolio valuation with market value, P&L, and weight per position"
        columns:
          - name: ticker
            tests:
              - unique
              - not_null
              - relationships:
                  to: ref('stg_holdings')
                  field: ticker
          - name: market_value
            tests:
              - not_null

      - name: sector_allocation
        description: "Sector-level portfolio allocation and P&L"
        columns:
          - name: sector
            tests:
              - unique
              - not_null
""")

Path(f"{PROJECT_DIR}/models/schema.yml").write_text(schema_yml)
print("Created: models/schema.yml")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Run dbt!
# MAGIC
# MAGIC Now we run the two core dbt commands:
# MAGIC
# MAGIC 1. **`dbt run`** — Execute all models in dependency order (compile SQL → run against warehouse)
# MAGIC 2. **`dbt test`** — Run all tests defined in `schema.yml`
# MAGIC
# MAGIC In production, these commands run from your terminal or CI/CD pipeline.
# MAGIC Here we run them from within the notebook using `os.system()`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Verify the Project
# MAGIC Let's first check that dbt can parse our project and find all models.

# COMMAND ----------

os.chdir(PROJECT_DIR)
exit_code = os.system("dbt debug --profiles-dir . --project-dir .")
print(f"\ndbt debug exit code: {exit_code}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 `dbt run` — Execute All Models
# MAGIC
# MAGIC dbt will:
# MAGIC 1. Parse all `.sql` files and discover the DAG from `ref()` / `source()` calls
# MAGIC 2. Execute them in topological order: staging first, then marts
# MAGIC 3. Create views/tables in the `workspace.dbt_demo` schema
# MAGIC
# MAGIC Watch the output — it shows the exact SQL being executed and the time each
# MAGIC model takes.

# COMMAND ----------

exit_code = os.system("dbt run --profiles-dir . --project-dir .")
print(f"\ndbt run exit code: {exit_code}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Inspect the Results
# MAGIC
# MAGIC dbt created these objects in `workspace.dbt_demo`:
# MAGIC - `stg_holdings` → **view** (staging, lightweight)
# MAGIC - `stg_prices` → **view** (staging, lightweight)
# MAGIC - `portfolio_valuation` → **table** (mart, materialized for fast queries)
# MAGIC - `sector_allocation` → **table** (mart, materialized for fast queries)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check what dbt created
# MAGIC SHOW TABLES IN workspace.dbt_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect the portfolio valuation (our Gold table)
# MAGIC SELECT * FROM workspace.dbt_demo.portfolio_valuation
# MAGIC ORDER BY market_value DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect sector allocation
# MAGIC SELECT * FROM workspace.dbt_demo.sector_allocation

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 `dbt test` — Validate Data Quality
# MAGIC
# MAGIC Now run the tests defined in `schema.yml`. Each test is compiled to a SQL
# MAGIC query that should return **zero rows**. If any rows are returned, the test fails.
# MAGIC
# MAGIC For example, the `unique` test on `stg_holdings.ticker` compiles to:
# MAGIC ```sql
# MAGIC SELECT ticker
# MAGIC FROM workspace.dbt_demo.stg_holdings
# MAGIC GROUP BY ticker
# MAGIC HAVING COUNT(*) > 1
# MAGIC ```
# MAGIC If this returns any rows, there are duplicate tickers → test fails.

# COMMAND ----------

exit_code = os.system("dbt test --profiles-dir . --project-dir .")
print(f"\ndbt test exit code: {exit_code}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: The DAG
# MAGIC
# MAGIC dbt built this DAG automatically from your `source()` and `ref()` calls:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────┐     ┌────────────────┐     ┌───────────────────────┐
# MAGIC │ source:             │     │ staging:       │     │ marts:                │
# MAGIC │ bronze_holdings     │ ──→ │ stg_holdings   │ ──┐ │                       │
# MAGIC └─────────────────────┘     └────────────────┘   ├→│ portfolio_valuation   │
# MAGIC ┌─────────────────────┐     ┌────────────────┐   │ └───────────────────────┘
# MAGIC │ source:             │     │ staging:       │ ──┘           │
# MAGIC │ bronze_prices       │ ──→ │ stg_prices     │               ▼
# MAGIC └─────────────────────┘     └────────────────┘   ┌───────────────────────┐
# MAGIC                                                  │ sector_allocation     │
# MAGIC                                                  └───────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC Compare this to the DLT DAG from notebook 6 — **same structure, same data,
# MAGIC completely different tooling.**

# COMMAND ----------

# MAGIC %md
# MAGIC You can also generate the DAG visually:

# COMMAND ----------

# Generate and display the dbt DAG as JSON
exit_code = os.system("dbt ls --profiles-dir . --project-dir . --resource-type model --output json")
print(f"\ndbt ls exit code: {exit_code}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: The Same Pipeline, Four Ways
# MAGIC
# MAGIC You have now seen the same custodian pipeline built four different ways.
# MAGIC Here is how they compare:
# MAGIC
# MAGIC | Aspect | Imperative (nb5) | DLT (nb6) | dbt Core (nb7) | Raw SQL Scripts |
# MAGIC |---|---|---|---|---|
# MAGIC | **Language** | PySpark | PySpark + decorators | Pure SQL | Pure SQL |
# MAGIC | **DAG** | Manual (Workflow UI) | Auto (`dlt.read()`) | Auto (`ref()`) | Manual (script order) |
# MAGIC | **Tests** | None built-in | `@dlt.expect` | YAML tests (`unique`, `not_null`) | None |
# MAGIC | **Docs** | Comments only | Comments only | Auto-generated site | None |
# MAGIC | **Portability** | Databricks only | Databricks only | Any warehouse | Any warehouse |
# MAGIC | **Vendor lock-in** | High | High | None | None |
# MAGIC | **Orchestration** | Workflows | Pipeline engine | External (Airflow, etc.) | External |
# MAGIC | **Materialization** | You write `.saveAsTable()` | Platform handles it | Config in YAML | You write DDL |
# MAGIC | **Learning curve** | PySpark API | PySpark + DLT API | SQL + dbt concepts | SQL only |
# MAGIC | **Best for** | Custom logic, ML | Production Databricks | Multi-warehouse orgs | Quick prototypes |
# MAGIC
# MAGIC ### Other Approaches Worth Knowing
# MAGIC
# MAGIC | Tool | What It Is | Pros | Cons |
# MAGIC |------|-----------|------|------|
# MAGIC | **dbt Cloud** | Hosted dbt with IDE, scheduler, CI/CD | No infrastructure to manage, team features | Costs money, still SQL-only transforms |
# MAGIC | **Dataform** | Google's dbt-like tool (built into BigQuery) | Free with BigQuery, SQLX syntax | BigQuery-only, smaller community |
# MAGIC | **Apache Airflow** | DAG-based orchestrator | Handles any task (Python, SQL, APIs, ML) | Complex setup, overkill for SQL-only transforms |
# MAGIC | **SQLMesh** | Open-source dbt alternative | Virtual environments, column-level lineage | Newer, smaller community |
# MAGIC | **Stored Procedures** | Database-native transform scripts | No external tools, familiar to DBAs | No testing, no docs, no version control, no DAG |
# MAGIC
# MAGIC ### When to Use What
# MAGIC
# MAGIC - **dbt Core**: Your org uses multiple warehouses, or you want open-source with CI/CD
# MAGIC - **DLT**: You're all-in on Databricks and want platform-managed everything
# MAGIC - **Imperative PySpark**: You need custom logic (ML, APIs, complex Python)
# MAGIC - **Raw SQL**: Quick one-off analysis, no pipeline needed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Exercises
# MAGIC
# MAGIC ### Exercise 1: Add a New Model
# MAGIC
# MAGIC Create a new mart model called `top_gainers.sql` that selects only positions
# MAGIC where `unrealized_pnl > 0`, ordered by unrealized P&L descending. Use
# MAGIC `{{ ref('portfolio_valuation') }}` as the source.
# MAGIC
# MAGIC After creating the file, run `dbt run` again. Does dbt pick up the new model
# MAGIC and add it to the DAG automatically?
# MAGIC
# MAGIC ### Exercise 2: Add a Singular Test
# MAGIC
# MAGIC Create a file `tests/assert_portfolio_weights_sum_to_100.sql`:
# MAGIC ```sql
# MAGIC -- This test passes if it returns zero rows.
# MAGIC -- It fails if the portfolio weights don't sum to ~100%.
# MAGIC SELECT
# MAGIC     SUM(weight_pct) AS total_weight,
# MAGIC     ABS(SUM(weight_pct) - 100) AS drift
# MAGIC FROM {{ ref('portfolio_valuation') }}
# MAGIC HAVING ABS(SUM(weight_pct) - 100) > 0.5
# MAGIC ```
# MAGIC
# MAGIC Run `dbt test`. Does it pass?
# MAGIC
# MAGIC ### Exercise 3: Reflection
# MAGIC
# MAGIC 1. **DAG discovery**: Both DLT and dbt build the DAG automatically.
# MAGIC    What mechanism does each use? How are `dlt.read()` and `{{ ref() }}`
# MAGIC    conceptually similar?
# MAGIC
# MAGIC 2. **Testing philosophy**: In DLT, `@dlt.expect_or_drop()` silently drops
# MAGIC    bad rows. In dbt, test failures stop the pipeline. Which approach is
# MAGIC    safer? Which is more flexible? When would you want each?
# MAGIC
# MAGIC 3. **Vendor lock-in**: Your company uses Databricks today but is evaluating
# MAGIC    Snowflake. Which parts of your work from this module would transfer to
# MAGIC    Snowflake? Which would not? What does this tell you about tool selection?
# MAGIC
# MAGIC 4. **The "T" only**: dbt does not extract or load data. In our pipeline,
# MAGIC    what tool handled the "E" and "L" steps? (Hint: look at nb5a.)
# MAGIC    In a production system, what tools might handle E and L?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Optionally remove the dbt demo schema to free up catalog space:

# COMMAND ----------

# Uncomment the lines below to clean up:
# spark.sql("DROP SCHEMA IF EXISTS workspace.dbt_demo CASCADE")
# print("Dropped schema: workspace.dbt_demo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What Just Happened
# MAGIC
# MAGIC You built the same Medallion pipeline from notebooks 5 and 6, but this time
# MAGIC using **dbt Core** — an open-source, SQL-first transformation framework.
# MAGIC
# MAGIC Key takeaways:
# MAGIC
# MAGIC 1. **dbt models are just SQL `SELECT` statements.** No PySpark, no decorators,
# MAGIC    no `saveAsTable()`. dbt handles the DDL.
# MAGIC
# MAGIC 2. **`ref()` builds the DAG.** Same concept as `dlt.read()` in notebook 6.
# MAGIC    You declare what you need, and the tool resolves execution order.
# MAGIC
# MAGIC 3. **Tests are first-class citizens.** `unique`, `not_null`, `accepted_values`,
# MAGIC    `relationships` — declared in YAML, run with `dbt test`.
# MAGIC
# MAGIC 4. **dbt is warehouse-agnostic.** The same models would work on Snowflake,
# MAGIC    BigQuery, or Redshift with only a `profiles.yml` change.
# MAGIC
# MAGIC 5. **Software engineering for data.** dbt brings version control, testing,
# MAGIC    documentation, and CI/CD to data transformation — practices that have been
# MAGIC    standard in application development for decades.
# MAGIC
# MAGIC > **The bigger pattern**: Every distributed system we've studied this semester
# MAGIC > benefits from declarative, testable, well-documented code. dbt is what happens
# MAGIC > when the data world catches up to that realization.
