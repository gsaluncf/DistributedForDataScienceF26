# Databricks notebook source
# MAGIC %md
# MAGIC # Vectorized UDF: Simple ML scoring at scale
# MAGIC
# MAGIC This notebook shows a simple pattern you will see in real Spark work:
# MAGIC
# MAGIC - You have a table of rows (features)
# MAGIC - You have a trained model (represented here as a few numbers)
# MAGIC - You want to score every row efficiently
# MAGIC
# MAGIC We will use a **vectorized UDF** (a pandas UDF). The key idea is that Spark sends data to Python in **batches** as pandas Series, not one row at a time.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Create a small sample DataFrame
# MAGIC
# MAGIC In a real job, this would come from a table, files in a lake, or a streaming source.

# COMMAND ----------

# DBTITLE 1,Create a small sample DataFrame
import numpy as np

def generate_rows(n=10000, seed=42):
    np.random.seed(seed)
    ages = np.random.randint(18, 65, size=n)
    income_ks = np.random.uniform(20, 150, size=n)
    # Ensure native Python types for schema inference
    return [(int(age), float(inc)) for age, inc in zip(ages, income_ks)]

rows = generate_rows(20000)
df = spark.createDataFrame(rows, ["age", "income_k"])

display(df.limit(100))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Pretend we already trained a model
# MAGIC
# MAGIC We will use a tiny logistic regression style model:
# MAGIC
# MAGIC `z = W_age * age + W_inc * income_k + bias`
# MAGIC
# MAGIC `p = sigmoid(z)`
# MAGIC
# MAGIC These constants stand in for "trained weights".

# COMMAND ----------

W_AGE = 0.03
W_INC = 0.04
BIAS = -4.0

print("W_AGE =", W_AGE)
print("W_INC =", W_INC)
print("BIAS  =", BIAS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Define a vectorized UDF (pandas UDF)
# MAGIC
# MAGIC A pandas UDF receives whole *batches* of values as pandas Series.
# MAGIC That is the point: batch processing is far faster than calling Python once per row.

# COMMAND ----------

import numpy as np
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("double")
def predict_buy_prob(age: pd.Series, income_k: pd.Series) -> pd.Series:
    z = (W_AGE * age) + (W_INC * income_k) + BIAS
    return 1.0 / (1.0 + np.exp(-z))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Apply the function to score every row
# MAGIC
# MAGIC This creates a new column `p_buy` for every row in the DataFrame.

# COMMAND ----------

scored = df.withColumn("p_buy", predict_buy_prob(col("age"), col("income_k")))
scored.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) What to notice
# MAGIC
# MAGIC - Spark distributes the DataFrame across the cluster (partitions).
# MAGIC - Each worker sends a batch of `age` and `income_k` to Python.
# MAGIC - The UDF returns a batch of probabilities.
# MAGIC - Spark assembles the results back into one DataFrame.

# COMMAND ----------

# Optional: view the logical and physical plan.
# For bigger data, this helps you see where work happens.
scored.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6) One extra step: sort by probability
# MAGIC
# MAGIC This is a small example, but it looks like a real scoring workflow.

# COMMAND ----------

scored.orderBy(col("p_buy").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Testing Spark Parallelism with Different Partition Counts
# MAGIC
# MAGIC The function below lets us run the same computation while changing the number of Spark partitions.  
# MAGIC This helps demonstrate two key Spark ideas.
# MAGIC
# MAGIC **Lazy Evaluation**
# MAGIC
# MAGIC Spark does not execute transformations immediately.  
# MAGIC When we add a column with the UDF, Spark only builds an execution plan.  
# MAGIC The real computation happens later when an **action** is triggered.
# MAGIC
# MAGIC In this example the action is `collect()`, which forces Spark to run the entire pipeline.
# MAGIC
# MAGIC **Parallel Execution**
# MAGIC
# MAGIC Spark splits data into **partitions**.  
# MAGIC Each partition can run as a separate **task** across available CPU cores or worker nodes.
# MAGIC
# MAGIC By changing the partition count we can observe:
# MAGIC
# MAGIC - how many tasks Spark creates
# MAGIC - how the runtime changes
# MAGIC - how the Spark UI displays parallel work
# MAGIC
# MAGIC The function runs three stages and prints timing for each:
# MAGIC
# MAGIC 1. **Repartition** — distributes the data across partitions  
# MAGIC 2. **UDF column creation** — builds the logical plan (still lazy)  
# MAGIC 3. **Aggregation with collect()** — triggers execution of the full pipeline
# MAGIC
# MAGIC While the function runs, open the **Spark UI → Jobs → Stages → Tasks** to see how Spark distributes the work.

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum, spark_partition_id, countDistinct
import time

def run_scoring_test(partitions):

    print(f"\nRunning test with {partitions} partitions\n")

    # Step 1: Force partitions (work happens later; this is still mostly plan setup)
    start1 = time.time()
    df_p = df.repartition(partitions)
    end1 = time.time()
    print(f"Step 1 (repartition call / plan): {end1 - start1:.3f} seconds")

    # Step 2: Use the vectorized UDF exactly as-is (still lazy)
    start2 = time.time()
    scored = df_p.withColumn("p_buy", predict_buy_prob(col("age"), col("income_k")))
    end2 = time.time()
    print(f"Step 2 (UDF column / lazy plan): {end2 - start2:.3f} seconds")

    # Step 3: Force execution and also report partitions actually used
    start3 = time.time()
    result = (
        scored
        .withColumn("pid", spark_partition_id())
        .agg(
            spark_sum("p_buy").alias("total_prob"),
            countDistinct("pid").alias("partitions_used")
        )
        .collect()
    )
    end3 = time.time()
    print(f"Step 3 (execution): {end3 - start3:.3f} seconds")

    print("Result:", result)

# COMMAND ----------

rows = generate_rows(5000000)
df = spark.createDataFrame(rows, ["age", "income_k"])
print(f"Created DataFrame with {df.count()} rows")


run_scoring_test(1)
run_scoring_test(32)
run_scoring_test(128)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes for performance intuition
# MAGIC
# MAGIC - Prefer built-in Spark SQL functions when possible.
# MAGIC - If you need custom logic in Python, a pandas UDF is usually the next best option.
# MAGIC - Plain (row-by-row) Python UDFs can be much slower.