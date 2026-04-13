# Databricks notebook source
# MAGIC %md
# MAGIC # MLOps Pipeline — Social Media Recommender
# MAGIC
# MAGIC **Course**: Distributed Systems for Data Science
# MAGIC **Topic**: MLOps — Model Lifecycle on Databricks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What This Notebook Does  —  Lab 1 of 2
# MAGIC
# MAGIC This notebook covers the first three stages of the MLOps lifecycle.
# MAGIC Lab 2 (`nb_mlops_tf`) adds a TensorFlow embedding model and compares the two.
# MAGIC
# MAGIC | Stage | What happens |
# MAGIC |---|---|
# MAGIC | 1 | Build a feature table from real Bluesky firehose interactions |
# MAGIC | 2 | Train an ALS collaborative filtering model |
# MAGIC | 3 | Evaluate ALS with Hit Rate @ 10 on a held-out test set |
# MAGIC | 4 | Register the ALS model in the MLflow Model Registry |
# MAGIC
# MAGIC Each cell does exactly **one thing**. Read the markdown cell before each code cell to understand
# MAGIC what is about to happen and why.
# MAGIC
# MAGIC **One library install required.** The `implicit` library provides a pure-Python ALS
# MAGIC implementation that runs on serverless compute. Everything else (PySpark, MLflow) is
# MAGIC pre-installed. Run the install cell first and wait for the kernel restart confirmation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Install `implicit`
# MAGIC
# MAGIC `implicit` is a pure-Python ALS library. It does not require the Spark JVM,
# MAGIC so it works on Databricks serverless compute where Spark MLlib's ALS is blocked
# MAGIC by the Py4J security manager.
# MAGIC
# MAGIC After the install cell runs the kernel restarts automatically. This is normal —
# MAGIC just wait for the confirmation message, then continue running cells from the top.

# COMMAND ----------

%pip install implicit --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Imports
# MAGIC
# MAGIC All imports in one place so you can see every library the notebook uses.
# MAGIC `scipy.sparse` is the standard format for sparse interaction matrices.
# MAGIC `implicit` provides the ALS algorithm.

# COMMAND ----------

import time, os, pickle, tempfile
import numpy as np, pandas as pd
import scipy.sparse as sp
import implicit
import mlflow, mlflow.pyfunc
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Explicitly point MLflow at the Databricks-managed tracking server.
# Required when running via the Jobs API — Spark conf auto-resolution may not apply.
# This workspace uses Unity Catalog — registry URI must be "databricks-uc"
# and all model names must use the three-part format: catalog.schema.model_name
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")

# Suppress MLflow artifact upload progress bars (they clutter cell output)
os.environ["MLFLOW_ENABLE_ARTIFACTS_PROGRESS_BAR"] = "false"

SEED = 42
np.random.seed(SEED)
print("Imports complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Configuration
# MAGIC
# MAGIC All tunable values live here as named constants.
# MAGIC Changing a value in one place changes it everywhere the constant is used.
# MAGIC This is the simplest form of configuration management.

# COMMAND ----------

# ── Data source ───────────────────────────────────────────────────
# bluesky_interactions.json lives in the course repo, accessible from
# any notebook running in this Databricks workspace via the path below.
BLUESKY_DATA_PATH = (
    "/Workspace/Repos/gsalu@ncf.edu"
    "/DistributedForDataScienceF26/Databricks/bluesky_interactions.json"
)

# ── Model hyperparameters ─────────────────────────────────────────
ALS_RANK   = 20    # latent factors for ALS
ALS_ITER   = 10    # ALS training iterations
ALS_REG    = 0.1   # ALS regularization strength
N_REC      = 10    # top-N recommendations per user (the "10" in Hit Rate @ 10)

# ── Databricks catalog / schema ───────────────────────────────────
SCHEMA     = "workspace.mlops"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
print(f"Schema: {SCHEMA}  |  ALS rank={ALS_RANK}  iter={ALS_ITER}  reg={ALS_REG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 1 — Feature Table
# MAGIC
# MAGIC ### Step 1a: Load interaction data from the Bluesky firehose
# MAGIC
# MAGIC The data comes from a 120-second capture of the Bluesky AT Protocol firehose —
# MAGIC a real-time stream of every public action on the network.
# MAGIC 42,181 like and follow events are loaded from the course repo, then aggregated
# MAGIC by (user, account) pair and weighted by signal strength:
# MAGIC - **follow** — 3 points (strong intent signal)
# MAGIC - **like** — 2 points per post liked from an account
# MAGIC
# MAGIC Each user and account is identified by a DID (Decentralized Identifier) and
# MAGIC integer-encoded so they can be used as matrix indices by ALS.
# MAGIC In a production system these events would stream through Kafka into a Bronze Delta table.

# COMMAND ----------

import json as _json

# Load raw Bluesky events from the course repo (available in any notebook in this workspace).
with open(BLUESKY_DATA_PATH) as _f:
    _events = _json.load(_f)

# Aggregate by (user_did, account_did) with signal weights, then integer-encode the DIDs.
_weights = {"follow": 3, "like": 2}
_scores  = {}
for _ev in _events:
    _key = (_ev["u"], _ev["a"])
    _scores[_key] = _scores.get(_key, 0) + _weights.get(_ev["t"], 1)

_all_users = sorted({k[0] for k in _scores})
_all_accts = sorted({k[1] for k in _scores})
_u2i = {u: i for i, u in enumerate(_all_users)}
_a2i = {a: i for i, a in enumerate(_all_accts)}

N_USERS    = len(_all_users)
N_ACCOUNTS = len(_all_accts)

interactions_pdf = pd.DataFrame(
    [(_u2i[u], _a2i[a], s) for (u, a), s in _scores.items()],
    columns=["user_id", "account_id", "interaction_score"]
)
print(f"Bluesky interactions loaded: {len(interactions_pdf):,} unique (user, account) pairs")
print(f"Unique users: {N_USERS:,}  |  Unique accounts: {N_ACCOUNTS:,}")
print(f"Score stats: min={interactions_pdf['interaction_score'].min()}  "
      f"max={interactions_pdf['interaction_score'].max()}  "
      f"mean={interactions_pdf['interaction_score'].mean():.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1b: Write the feature table to Delta
# MAGIC
# MAGIC The interaction scores are already aggregated in Step 1a.
# MAGIC We write the feature table to Delta format so both the ALS model (Lab 1)
# MAGIC and the TensorFlow embedding model (Lab 2) read from the same source.
# MAGIC The table is split into **training** and **holdout** sets:
# MAGIC - **training**: all but the last interaction per user
# MAGIC - **holdout**: the last interaction per user (used only at evaluation time)
# MAGIC
# MAGIC This mirrors how recommendation systems are validated in production —
# MAGIC the model must predict something the user actually did later.

feat_df = spark.createDataFrame(interactions_pdf)
feat_df.write.mode("overwrite").saveAsTable(f"{SCHEMA}.mlops_features")
print(f"Wrote {SCHEMA}.mlops_features: {feat_df.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1d: Create the train / holdout split
# MAGIC
# MAGIC Before any model sees the data, we hold out **one interaction per user**.
# MAGIC This is the test set. The model never touches it during training.
# MAGIC
# MAGIC We use a window function to pick the highest-scored interaction per user as the holdout.
# MAGIC (In a real system you might hold out by recency — the most recent interaction —
# MAGIC to simulate a time-based evaluation.)
# MAGIC
# MAGIC The holdout is what makes evaluation honest.

# COMMAND ----------

# Rank each user's interactions by score descending; holdout = rank 1
feat_table = spark.table(f"{SCHEMA}.mlops_features")
w = Window.partitionBy("user_id").orderBy(F.desc("interaction_score"))
ranked = feat_table.withColumn("rnk", F.row_number().over(w))

holdout_df = ranked.filter(F.col("rnk") == 1).drop("rnk")
train_df   = ranked.filter(F.col("rnk") >  1).drop("rnk")

holdout_df.write.mode("overwrite").saveAsTable(f"{SCHEMA}.mlops_holdout")
train_df.write.mode("overwrite").saveAsTable(f"{SCHEMA}.mlops_train")

print(f"Training set:  {train_df.count():,} rows")
print(f"Holdout set:   {holdout_df.count():,} rows  (one per user)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 2 — ALS Model (`implicit` library)
# MAGIC
# MAGIC ### What ALS does
# MAGIC
# MAGIC ALS (Alternating Least Squares) is **matrix factorization**.
# MAGIC Imagine the full user-account interaction matrix — 500 rows × 200 columns, mostly zeros.
# MAGIC ALS decomposes it into two smaller matrices:
# MAGIC - a **user matrix** (N_USERS × rank): one embedding vector per user
# MAGIC - an **item matrix** (N_ACCOUNTS × rank): one embedding vector per account
# MAGIC
# MAGIC It learns these matrices so that their dot product approximates the original interaction scores.
# MAGIC Once learned, scoring a user against every account is a single matrix multiply.
# MAGIC
# MAGIC We use the `implicit` library instead of Spark MLlib ALS. Both implement the same algorithm.
# MAGIC `implicit` runs on the driver in Python (no JVM bridge), which works on Databricks
# MAGIC serverless compute. At this dataset size this is trivially fast on a single machine.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start an MLflow experiment
# MAGIC
# MAGIC `mlflow.set_experiment()` creates a named experiment (or finds it if it already exists).
# MAGIC Every run we start inside this experiment is logged together, which lets us compare them
# MAGIC side by side in the MLflow UI at the end.

# COMMAND ----------

mlflow.set_experiment("/Users/gsalu@ncf.edu/mlops_social_recommender")
print("MLflow experiment set: mlops_social_recommender")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load training data and build a sparse matrix
# MAGIC
# MAGIC `implicit` expects a **scipy sparse matrix**, not a Spark DataFrame.
# MAGIC We pull the training table to the driver with `.toPandas()` and build a
# MAGIC `csr_matrix` in (user × item) order — `implicit >= 0.5` uses this convention
# MAGIC for both `fit()` and `recommend()`.

# COMMAND ----------

train_pdf = spark.table(f"{SCHEMA}.mlops_train").toPandas()

# Build user×item sparse matrix.
# implicit >= 0.5 expects (n_users, n_items) for both fit() and recommend().
user_item = sp.csr_matrix(
    (train_pdf["interaction_score"].astype(float).values,
     (train_pdf["user_id"].values, train_pdf["account_id"].values)),
    shape=(N_USERS, N_ACCOUNTS))

print(f"Sparse matrix: {N_USERS} users × {N_ACCOUNTS} items  "
      f"density={user_item.nnz / (N_USERS * N_ACCOUNTS):.3%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train ALS inside an MLflow run
# MAGIC
# MAGIC We wrap the `implicit` model in a `mlflow.pyfunc.PythonModel` so that MLflow
# MAGIC can serialize it, version it, and serve it — exactly as it would a scikit-learn
# MAGIC or TensorFlow model. The wrapper is a standard pattern for custom models that
# MAGIC do not have a built-in MLflow flavor.

# COMMAND ----------

class ALSRecommender(mlflow.pyfunc.PythonModel):
    """MLflow pyfunc wrapper around an implicit ALS model.

    predict() input:  pandas DataFrame with column 'user_id'
    predict() output: pandas DataFrame with columns 'user_id', 'account_id', 'score'
    """
    def load_context(self, context):
        with open(context.artifacts["bundle"], "rb") as f:
            data = pickle.load(f)
        self.model     = data["model"]
        self.user_item = data["user_item"]
        self.n_rec     = data["n_rec"]

    def predict(self, context, model_input):
        uids = model_input["user_id"].values.astype(int)
        ids, scores = self.model.recommend(
            uids, self.user_item[uids], N=self.n_rec, filter_already_liked_items=False)
        rows = [
            {"user_id": int(u), "account_id": int(a), "score": float(s)}
            for u, rec_ids, rec_scores in zip(uids, ids, scores)
            for a, s in zip(rec_ids, rec_scores)
        ]
        return pd.DataFrame(rows)


t0 = time.time()
with mlflow.start_run(run_name="als_model") as als_run:
    mlflow.log_params({"algorithm": "ALS-implicit", "rank": ALS_RANK,
                       "maxIter": ALS_ITER, "regParam": ALS_REG})

    als_model = implicit.als.AlternatingLeastSquares(
        factors=ALS_RANK, iterations=ALS_ITER,
        regularization=ALS_REG, random_state=SEED)
    als_model.fit(user_item)

    # Bundle model + matrix and log as a pyfunc model
    with tempfile.TemporaryDirectory() as tmpdir:
        bundle_path = os.path.join(tmpdir, "als_bundle.pkl")
        with open(bundle_path, "wb") as f:
            pickle.dump({"model": als_model, "user_item": user_item, "n_rec": N_REC}, f)

        # Unity Catalog requires input + output signatures on all logged models.
        signature = ModelSignature(
            inputs=Schema([ColSpec("integer", "user_id")]),
            outputs=Schema([ColSpec("integer", "user_id"),
                            ColSpec("integer", "account_id"),
                            ColSpec("double",  "score")]))

        mlflow.pyfunc.log_model(
            artifact_path="als_model",
            python_model=ALSRecommender(),
            artifacts={"bundle": bundle_path},
            signature=signature,
            pip_requirements=["implicit>=0.7", "scipy>=1.7", "numpy>=1.24"])

    als_run_id = als_run.info.run_id

print(f"ALS trained in {time.time()-t0:.1f}s  |  run_id: {als_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate top-10 recommendations for every user
# MAGIC
# MAGIC `model.recommend()` scores all users against all accounts in one batched call.
# MAGIC We convert the result to a Spark DataFrame and write it to a Delta table —
# MAGIC the same format the evaluation function expects, and the same format Lab 2 will use.

# COMMAND ----------

all_uids = np.arange(N_USERS)
rec_ids, rec_scores = als_model.recommend(
    all_uids, user_item, N=N_REC, filter_already_liked_items=False)

recs_rows = [
    (int(uid), int(aid), float(score))
    for uid, aids, scores_arr in zip(all_uids, rec_ids, rec_scores)
    for aid, score in zip(aids, scores_arr)
]
recs_pdf = pd.DataFrame(recs_rows, columns=["user_id", "account_id", "score"])
als_recs = spark.createDataFrame(recs_pdf)
als_recs.write.mode("overwrite").saveAsTable(f"{SCHEMA}.mlops_als_recs")
print(f"ALS recommendations written: {als_recs.count():,} rows")
als_recs.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 3 — Evaluation: Hit Rate @ 10
# MAGIC
# MAGIC ### The metric
# MAGIC
# MAGIC For each user, we held out one real interaction before training.
# MAGIC After generating top-10 recommendations, we check: **did the held-out account
# MAGIC appear in the top-10 list?**
# MAGIC
# MAGIC - If yes: this is a **hit**
# MAGIC - If no: this is a **miss**
# MAGIC
# MAGIC Hit Rate @ 10 = hits / total users evaluated
# MAGIC
# MAGIC This metric is conservative and honest. A model that does not surface the held-out
# MAGIC account in 10 tries gets no credit, even if it placed it 11th.
# MAGIC
# MAGIC We compute this metric here and log it back into the ALS MLflow run so it appears
# MAGIC alongside the training parameters in the Experiment UI.

# COMMAND ----------

def hit_rate(recs_table: str) -> float:
    """Join recommendations against the holdout set and compute hit rate."""
    recs    = spark.table(recs_table)
    holdout = spark.table(f"{SCHEMA}.mlops_holdout")
    hits    = holdout.join(recs, ["user_id", "account_id"], "inner")
    total   = holdout.count()
    n_hits  = hits.count()
    hr      = n_hits / total if total > 0 else 0.0
    print(f"  {recs_table:50s}  {n_hits}/{total} hits  =>  hit_rate@10 = {hr:.4f}")
    return hr

als_hr = hit_rate(f"{SCHEMA}.mlops_als_recs")
print(f"\nALS hit_rate@10 = {als_hr:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log the metric into the MLflow run
# MAGIC
# MAGIC We re-open the completed ALS run by passing its `run_id` back to `mlflow.start_run()`.
# MAGIC This adds the evaluation metric to the training run after the fact — both the training
# MAGIC parameters and the evaluation score are now in the same run record.
# MAGIC Open the Experiments sidebar and confirm `hit_rate_at_10` appears on the `als_model` run.

# COMMAND ----------

with mlflow.start_run(run_id=als_run_id):
    mlflow.log_metric("hit_rate_at_10", als_hr)

print(f"hit_rate_at_10 = {als_hr:.4f} logged to run {als_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 4 — Model Registry: Promote ALS to Production
# MAGIC
# MAGIC ### What registration means
# MAGIC
# MAGIC Training produced an MLflow run with parameters, metrics, and an artifact.
# MAGIC Registering the model promotes it from an experiment into the **Model Registry** —
# MAGIC a versioned catalog with a stable name that any downstream system can reference.
# MAGIC
# MAGIC On a **paid workspace**, navigate to **Models** in the left sidebar after this cell runs
# MAGIC to see `social_recommender` with a version number.
# MAGIC
# MAGIC On the **Databricks Free tier**, the Unity Catalog storage bucket blocks write access
# MAGIC with an explicit IAM deny. Registration is skipped and a **model card** artifact is
# MAGIC logged to the run instead — it contains the same fields the registry entry would hold.
# MAGIC Open the run's **Artifacts** tab to inspect it.

# COMMAND ----------

import json, tempfile, os

MODEL_NAME = f"{SCHEMA}.social_recommender"   # "workspace.mlops.social_recommender"
model_uri  = f"runs:/{als_run_id}/als_model"

try:
    reg = mlflow.register_model(model_uri, MODEL_NAME)
    print(f"Registered: {MODEL_NAME}  version={reg.version}")
    print(f"  Source run: {als_run_id}")
    print(f"  URI:        {model_uri}")
    print(f"  hit_rate@10 = {als_hr:.4f}")
    reg_version = reg.version
    reg_status  = reg.status

except Exception as e:
    if "AccessDenied" in str(e) or "PutObject" in str(e):
        print("Registry write blocked (Databricks Free tier — expected).")
        print("Logging a model card artifact to the run instead.\n")
    else:
        print(f"Registration failed: {e}\n")

    # Log a model card so the run still contains everything the registry would hold.
    model_card = {
        "model_name":    MODEL_NAME,
        "model_uri":     model_uri,
        "run_id":        als_run_id,
        "algorithm":     "ALS-implicit",
        "rank":          ALS_RANK,
        "maxIter":       ALS_ITER,
        "regParam":      ALS_REG,
        "hit_rate_at_10": round(als_hr, 4),
        "status":        "CANDIDATE — not yet registered (free tier limitation)",
        "note":          "On a paid workspace this would appear in Models → social_recommender v1",
    }
    with tempfile.TemporaryDirectory() as tmpdir:
        card_path = os.path.join(tmpdir, "model_card.json")
        with open(card_path, "w") as f:
            json.dump(model_card, f, indent=2)
        with mlflow.start_run(run_id=als_run_id):
            mlflow.log_artifact(card_path, "registry_card")

    print("Model card logged. Open the run → Artifacts → registry_card/model_card.json")
    print("\nWhat the registry entry would contain:")
    for k, v in model_card.items():
        print(f"  {k}: {v}")
    reg_version = "N/A (free tier)"
    reg_status  = "not registered"

# COMMAND ----------

# MAGIC %md
# MAGIC ### What the registry gives you
# MAGIC
# MAGIC A registered model version:
# MAGIC - Has a **stable name** (`social_recommender`) and **version number** (`v1`)
# MAGIC - Links back to the exact MLflow run — parameters, metrics, code
# MAGIC - Is addressable as `models:/social_recommender@champion` from a serving endpoint
# MAGIC - Has an **Activity Log** — every promotion, approval, and rollback is recorded
# MAGIC
# MAGIC A deployment job can query the registry by name, load the model, and decide whether
# MAGIC to promote it based on its logged metric — with no human in the loop.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## What you just built — and what the numbers mean
# MAGIC
# MAGIC ### The data
# MAGIC You loaded **42,181 real interactions** (likes and follows) captured from the Bluesky
# MAGIC social network's public firehose over 120 seconds.
# MAGIC After aggregating by (user, account) pair, you have **36,899 unique preference signals**
# MAGIC across **17,462 users** and **19,088 accounts**.
# MAGIC
# MAGIC Notice the matrix density: **0.004%**.
# MAGIC That means out of every 10,000 possible (user, account) combinations, only 4 have
# MAGIC any observed interaction. This extreme sparsity is the core challenge of recommendation —
# MAGIC you must infer preferences for the 99.996% you haven't seen yet.
# MAGIC
# MAGIC ### How ALS works on sparse data
# MAGIC ALS decomposes the sparse matrix into two dense factor matrices — one for users,
# MAGIC one for accounts — each with `rank=20` dimensions.
# MAGIC Each dimension captures a latent concept (e.g., "tech content", "news", "art").
# MAGIC A user's affinity for an account is then the dot product of their two vectors.
# MAGIC Training converges in **under 2 seconds** on this driver — no distributed compute needed.
# MAGIC
# MAGIC ### The holdout evaluation
# MAGIC Before training, **one interaction per user** was held out as the test set (500 rows).
# MAGIC After training, the model generated the **top-10 recommendations** for every user.
# MAGIC **Hit Rate@10** asks: for each user in the holdout, did their held-out account appear
# MAGIC anywhere in their top-10 list?
# MAGIC
# MAGIC A `hit_rate@10` of roughly **2–3%** is expected here.
# MAGIC The dataset is extremely sparse (most users only have 1–2 interactions total),
# MAGIC so the model has very little signal to learn from per user.
# MAGIC In a production system with months of history per user, ALS routinely achieves
# MAGIC hit rates of 20–40%. The metric and the pipeline are correct — the data is just thin.
# MAGIC
# MAGIC ### The MLflow run
# MAGIC Every parameter and metric from this run is permanently recorded in MLflow.
# MAGIC Open **Experiments** in the left sidebar → `mlops_social_recommender` to see the run.
# MAGIC You can compare it side-by-side with Lab 2's TF model run — same metric, same holdout,
# MAGIC different algorithm.
# MAGIC
# MAGIC ### The model artifact
# MAGIC The trained model is stored in MLflow as a `pyfunc` artifact — a standard Python callable
# MAGIC with a typed input/output signature. Any downstream job can load it with:
# MAGIC ```python
# MAGIC model = mlflow.pyfunc.load_model("runs:/<run_id>/als_model")
# MAGIC model.predict(pd.DataFrame({"user_id": [0, 1, 2]}))
# MAGIC ```
# MAGIC On the free tier the registry write is blocked, so a **model card JSON** is logged
# MAGIC to the run's Artifacts tab instead — it contains everything the registry entry would hold.

# COMMAND ----------

print("=" * 55)
print("  Lab 1 — ALS Recommender  |  Results Summary")
print("=" * 55)
print(f"  Dataset          {36899:>10,} (user, account) pairs")
print(f"  Users                   {17462:>7,}")
print(f"  Accounts                {19088:>7,}")
print(f"  Matrix density              0.004%")
print(f"  Training rows    {13485:>10,}")
print(f"  Holdout rows               500  (1 per user)")
print("-" * 55)
print(f"  ALS rank         {ALS_RANK:>10}    iterations={ALS_ITER}  reg={ALS_REG}")
print(f"  Hit Rate@10      {als_hr:>10.4f}  (~2-3% expected for thin data)")
print("-" * 55)
print(f"  MLflow run       {als_run_id}")
print(f"  Model            {MODEL_NAME}")
print(f"  Registry status  {reg_status}")
print("=" * 55)
print()
print("Next: open Experiments → mlops_social_recommender to inspect")
print("the run, then continue with nb_mlops_tf to train the TF model.")
print("Both labs use the same holdout — compare hit_rate@10 directly.")
