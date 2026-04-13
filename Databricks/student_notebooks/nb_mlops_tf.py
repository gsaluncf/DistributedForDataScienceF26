# Databricks notebook source
# MAGIC %md
# MAGIC # MLOps Lab 2 — TensorFlow Embedding Model
# MAGIC
# MAGIC **Course**: Distributed Systems for Data Science
# MAGIC **Topic**: MLOps — Model Lifecycle on Databricks
# MAGIC **Prerequisite**: Lab 1 (`nb_mlops`) must be complete. The feature table,
# MAGIC holdout set, and ALS recommendation table must already exist in `workspace.mlops`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC
# MAGIC | Stage | What happens |
# MAGIC |---|---|
# MAGIC | 1 | Install TensorFlow and load the feature table from Lab 1 |
# MAGIC | 2 | Train a Keras embedding model (two-tower dot product) |
# MAGIC | 3 | Generate top-10 recommendations and evaluate with Hit Rate @ 10 |
# MAGIC | 4 | Compare with the ALS model from Lab 1 |
# MAGIC | 5 | Register the winner — if TF wins, it becomes `social_recommender v2` |
# MAGIC
# MAGIC Each cell does exactly **one thing**. Read the markdown before each code cell.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Install TensorFlow
# MAGIC
# MAGIC TensorFlow is not pre-installed on Databricks serverless compute.
# MAGIC `%pip install` installs it and **automatically restarts the Python environment**.
# MAGIC This is expected — wait for the restart message, then run the remaining cells in order.

# COMMAND ----------

# MAGIC %pip install -q "tensorflow>=2.16,<2.18"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Imports

# COMMAND ----------

import time, os, shutil, json, tempfile, numpy as np, pandas as pd
import mlflow, mlflow.pyfunc
from mlflow.models import infer_signature
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Explicitly point MLflow at the Databricks-managed tracking server.
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks")

# Suppress MLflow artifact upload progress bars (they clutter cell output)
os.environ["MLFLOW_ENABLE_ARTIFACTS_PROGRESS_BAR"] = "false"

SEED = 42
np.random.seed(SEED)
print("Imports complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Configuration
# MAGIC
# MAGIC `N_USERS` and `N_ACCOUNTS` are derived from the feature table built in Lab 1
# MAGIC so the embedding layers are sized correctly for whatever dataset was used there.

# COMMAND ----------

N_FACTORS  = 32    # embedding dimension for the TF model
N_REC      = 10    # top-N recommendations (must match Lab 1)
SCHEMA     = "workspace.mlops"

# Read actual cardinality from the Lab 1 feature table.
# The embedding layer must cover every integer index in [0, max_id].
_stats = spark.table(f"{SCHEMA}.mlops_features").agg(
    F.max("user_id").alias("max_user"),
    F.max("account_id").alias("max_acct")
).collect()[0]
N_USERS    = int(_stats["max_user"]) + 1
N_ACCOUNTS = int(_stats["max_acct"]) + 1

print(f"Schema: {SCHEMA}  |  N_USERS={N_USERS}  N_ACCOUNTS={N_ACCOUNTS}  N_FACTORS={N_FACTORS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Verify Lab 1 Tables Exist
# MAGIC
# MAGIC The embedding model reads from the same feature and holdout tables built in Lab 1.
# MAGIC If any of these are missing, run Lab 1 first.

# COMMAND ----------

for tbl in ["mlops_train", "mlops_holdout", "mlops_als_recs"]:
    n = spark.table(f"{SCHEMA}.{tbl}").count()
    print(f"  {SCHEMA}.{tbl}: {n:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Set MLflow Experiment
# MAGIC
# MAGIC We write to the same experiment as Lab 1 so both the ALS run and the TF run
# MAGIC appear together in the Experiment UI. This is what enables side-by-side comparison.

# COMMAND ----------

mlflow.set_experiment("/Users/gsalu@ncf.edu/mlops_social_recommender")
print("MLflow experiment: mlops_social_recommender")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 1 — Prepare Training Data for the Embedding Model
# MAGIC
# MAGIC ### Pull the feature table to the driver
# MAGIC
# MAGIC The Keras model runs on the driver node (single machine), not on the Spark cluster.
# MAGIC We use `.toPandas()` to move the training data from distributed storage to driver memory.
# MAGIC For our dataset (< 15,000 rows) this is fine. For millions of rows, you would use
# MAGIC Horovod or TF on Spark to distribute gradient computation.

# COMMAND ----------

train_pd  = spark.table(f"{SCHEMA}.mlops_train").toPandas()
users_pos = train_pd["user_id"].values.astype("int32")
accts_pos = train_pd["account_id"].values.astype("int32")
max_score = train_pd["interaction_score"].max()
labels_pos = (train_pd["interaction_score"].values / max_score).astype("float32")

print(f"Training pairs loaded: {len(train_pd):,}")
print(f"Score range: 1 – {int(max_score)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Negative sampling
# MAGIC
# MAGIC The training set only contains real (positive) interactions.
# MAGIC The model also needs **negative examples** — user-account pairs that never interacted —
# MAGIC so it can learn to score non-interactions lower.
# MAGIC
# MAGIC For every positive pair, we sample one random pair that is NOT in the training set
# MAGIC and assign it label 0. The result is a balanced 50/50 positive-negative dataset.

# COMMAND ----------

known = set(zip(users_pos.tolist(), accts_pos.tolist()))
neg   = []
while len(neg) < len(train_pd):
    u, a = np.random.randint(0, N_USERS), np.random.randint(0, N_ACCOUNTS)
    if (u, a) not in known:
        neg.append((u, a))
neg = np.array(neg, dtype="int32")

all_u = np.concatenate([users_pos, neg[:, 0]])
all_a = np.concatenate([accts_pos, neg[:, 1]])
all_y = np.concatenate([labels_pos, np.zeros(len(neg), dtype="float32")])
idx   = np.random.permutation(len(all_u))
all_u, all_a, all_y = all_u[idx], all_a[idx], all_y[idx]

print(f"Total training pairs (positive + negative): {len(all_u):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 2 — Define and Train the Keras Embedding Model
# MAGIC
# MAGIC ### The architecture
# MAGIC
# MAGIC This is a **two-tower model**: one embedding lookup for users, one for accounts.
# MAGIC A dot product measures similarity between the two vectors.
# MAGIC A sigmoid activation converts the dot product to a probability.
# MAGIC
# MAGIC During training, backpropagation adjusts the embedding vectors so that
# MAGIC users and accounts that actually interact end up closer in the embedding space.

# COMMAND ----------

import tensorflow as tf
from tensorflow import keras

print(f"TensorFlow {tf.__version__}")

ui = keras.Input(shape=(1,), name="user_id")
ai = keras.Input(shape=(1,), name="account_id")

ue = keras.layers.Flatten()(keras.layers.Embedding(N_USERS,    N_FACTORS, name="user_emb")(ui))
ae = keras.layers.Flatten()(keras.layers.Embedding(N_ACCOUNTS, N_FACTORS, name="acct_emb")(ai))

out = keras.layers.Dense(1, activation="sigmoid")(keras.layers.Dot(axes=1)([ue, ae]))
tf_model = keras.Model(inputs=[ui, ai], outputs=out)
tf_model.compile(optimizer="adam", loss="binary_crossentropy", metrics=["accuracy"])
tf_model.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train and log to MLflow
# MAGIC
# MAGIC We open a new MLflow run in the same experiment as Lab 1.
# MAGIC Training parameters and the model artifact are logged inside the `with` block.
# MAGIC The run closes automatically when the block exits.

# COMMAND ----------

class EmbeddingRecommender(mlflow.pyfunc.PythonModel):
    """Pyfunc wrapper so the Keras model can be registered in the Model Registry."""
    def load_context(self, context):
        import tensorflow as tf
        self.model = tf.keras.models.load_model(context.artifacts["keras_model"])
    def predict(self, context, model_input, params=None):
        import numpy as np
        u = model_input["user_id"].values.astype("int32")
        a = model_input["account_id"].values.astype("int32")
        return self.model.predict([u, a], verbose=0).flatten()

t0 = time.time()
with mlflow.start_run(run_name="embedding_model") as emb_run:
    mlflow.log_params({"algorithm": "TF-Embedding-Keras", "n_factors": N_FACTORS,
                       "optimizer": "adam", "epochs": 20, "n_users": N_USERS,
                       "n_accounts": N_ACCOUNTS})

    tf_model.fit([all_u, all_a], all_y, epochs=20, batch_size=256,
                 verbose=1, validation_split=0.1)

    # Build a signature so the model is UC-compatible.
    _sig_in  = pd.DataFrame({"user_id": [0, 1], "account_id": [0, 1]})
    _sig_out = tf_model.predict(
        [np.array([0, 1], dtype="int32"), np.array([0, 1], dtype="int32")], verbose=0
    ).flatten()
    sig = infer_signature(_sig_in, _sig_out)

    # Keras 3 (ships with TF >=2.16) requires a .keras extension.
    tmp = "/tmp/embedding_model_export.keras"
    if os.path.exists(tmp): os.remove(tmp)
    tf_model.save(tmp)
    mlflow.pyfunc.log_model(
        artifact_path="embedding_model",
        python_model=EmbeddingRecommender(),
        artifacts={"keras_model": tmp},
        signature=sig,
    )
    emb_run_id = emb_run.info.run_id

print(f"TF trained in {time.time()-t0:.1f}s  |  run_id: {emb_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 3 — Generate Recommendations and Evaluate
# MAGIC
# MAGIC ### Score all (user, account) pairs
# MAGIC
# MAGIC We build a full grid of all 500 × 200 = 100,000 possible pairs,
# MAGIC run a single batched prediction, then use a Spark window function
# MAGIC to keep only the top-10 per user.

# COMMAND ----------

grid_u = np.repeat(np.arange(N_USERS,    dtype="int32"), N_ACCOUNTS)
grid_a = np.tile(  np.arange(N_ACCOUNTS, dtype="int32"), N_USERS)
scores = tf_model.predict([grid_u, grid_a], verbose=0).flatten()

emb_recs = (spark.createDataFrame(
                pd.DataFrame({"user_id": grid_u.astype(int),
                              "account_id": grid_a.astype(int),
                              "score": scores.astype(float)}))
    .withColumn("rnk", F.row_number().over(
        Window.partitionBy("user_id").orderBy(F.desc("score"))))
    .filter(F.col("rnk") <= N_REC).drop("rnk"))

emb_recs.write.mode("overwrite").saveAsTable(f"{SCHEMA}.mlops_emb_recs")
print(f"Embedding recommendations written: {emb_recs.count():,} rows")
emb_recs.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluate Hit Rate @ 10 for both models
# MAGIC
# MAGIC We use the same holdout set from Lab 1 to evaluate both models.
# MAGIC Comparing them on identical test data is what makes the metric meaningful.

# COMMAND ----------

def hit_rate(recs_table: str) -> float:
    recs    = spark.table(recs_table)
    holdout = spark.table(f"{SCHEMA}.mlops_holdout")
    n_hits  = holdout.join(recs, ["user_id", "account_id"], "inner").count()
    total   = holdout.count()
    hr      = n_hits / total if total > 0 else 0.0
    print(f"  {recs_table:50s}  {n_hits}/{total} hits  =>  {hr:.4f}")
    return hr

print("Hit Rate @ 10:")
als_hr = hit_rate(f"{SCHEMA}.mlops_als_recs")
emb_hr = hit_rate(f"{SCHEMA}.mlops_emb_recs")

with mlflow.start_run(run_id=emb_run_id):
    mlflow.log_metric("hit_rate_at_10", emb_hr)

print(f"\nMetric logged to embedding run: {emb_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 4 — Compare Models in the MLflow UI
# MAGIC
# MAGIC Before running the next cell, open **Experiments** in the left sidebar.
# MAGIC Find `mlops_social_recommender`. Select both the `als_model` run and the
# MAGIC `embedding_model` run. Click **Compare**.
# MAGIC
# MAGIC You can see all logged parameters and the `hit_rate_at_10` metric side by side.
# MAGIC This is the evidence-based model selection step.

# COMMAND ----------

print("=" * 60)
print("MODEL COMPARISON")
print("=" * 60)
print(f"  ALS        hit_rate@10 = {als_hr:.4f}  (Lab 1, registered as v1)")
print(f"  Embedding  hit_rate@10 = {emb_hr:.4f}  (Lab 2, this run)")
print()
if emb_hr > als_hr:
    print(f"TF embedding model wins by {emb_hr - als_hr:.4f}.")
    print("Will register as social_recommender v2.")
else:
    print(f"ALS holds as champion (tied or ahead by {als_hr - emb_hr:.4f}).")
    print("ALS v1 remains the current registered model.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 5 — Register the Winner
# MAGIC
# MAGIC If the TF embedding model beats ALS, we attempt to register it as `social_recommender v2`.
# MAGIC The version number auto-increments — v1 (ALS) is not deleted.
# MAGIC
# MAGIC If registration succeeds, navigate to **Models → social_recommender** to see both versions.
# MAGIC If the workspace blocks the write, a **model card** is logged as an artifact to the run instead —
# MAGIC open the run's **Artifacts** tab and find `registry_card/model_card.json`.
# MAGIC
# MAGIC If ALS is still champion, registration is skipped. This is how a deployment pipeline works:
# MAGIC it only promotes when the challenger definitively beats the champion.

# COMMAND ----------

MODEL_NAME = f"{SCHEMA}.social_recommender"
reg_version = "N/A"
reg_status  = "ALS holds as champion"

if emb_hr > als_hr:
    model_uri = f"runs:/{emb_run_id}/embedding_model"
    try:
        reg = mlflow.register_model(model_uri, MODEL_NAME)
        print(f"Registered: {MODEL_NAME}  version={reg.version}")
        print(f"  Source run:  {emb_run_id}")
        print(f"  hit_rate@10: {emb_hr:.4f}")
        reg_version = reg.version
        reg_status  = reg.status

    except Exception as e:
        if "AccessDenied" in str(e) or "PutObject" in str(e):
            print("Registry write blocked. Logging a model card artifact instead.\n")
        else:
            print(f"Registration note: {e}\n")

        model_card = {
            "model_name":         MODEL_NAME,
            "model_uri":          model_uri,
            "run_id":             emb_run_id,
            "algorithm":          "TF-Embedding-Keras",
            "n_factors":          N_FACTORS,
            "epochs":             20,
            "optimizer":          "adam",
            "hit_rate_at_10":     round(emb_hr, 4),
            "als_hit_rate_at_10": round(als_hr, 4),
            "decision":           "TF embedding wins — would register as social_recommender v2",
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            card_path = os.path.join(tmpdir, "model_card.json")
            with open(card_path, "w") as f:
                json.dump(model_card, f, indent=2)
            with mlflow.start_run(run_id=emb_run_id):
                mlflow.log_artifact(card_path, "registry_card")
        print("Model card logged → Artifacts → registry_card/model_card.json")
        reg_version = "N/A"
        reg_status  = "not registered"

else:
    print("ALS is still champion. No new version registered.")
    print("ALS remains the current social_recommender model.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What the registry gives you
# MAGIC
# MAGIC A registered model version has a stable name (`social_recommender`) and version number,
# MAGIC links back to the exact MLflow run, and has an **Activity Log** — every promotion,
# MAGIC approval, and rollback is recorded with timestamps and attribution.
# MAGIC
# MAGIC On a workspace where registration succeeded, navigate to **Models → social_recommender**
# MAGIC and click the **Activity** tab. You will see at least two events — one per registered version —
# MAGIC each timestamped and linked to the run that produced it.
# MAGIC
# MAGIC If the workspace logged a model card artifact instead, open the **embedding_model** run's
# MAGIC **Artifacts** tab. The `registry_card/model_card.json` file contains the same fields
# MAGIC the registry entry would hold: model name, run ID, algorithm, metric, and decision.

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## What you just built — and what the numbers mean
# MAGIC
# MAGIC ### The model architecture
# MAGIC The TF embedding model is a **two-tower dot-product network**.
# MAGIC It learns two separate embedding tables — one for users, one for accounts — each with
# MAGIC `N_FACTORS=32` dimensions. During training, it pulls the embedding vector for the user
# MAGIC and the embedding vector for the account they interacted with, computes their dot product,
# MAGIC and nudges the weights so that pairs with high interaction scores score higher than
# MAGIC randomly sampled pairs (this is implicit feedback learning with negative sampling).
# MAGIC
# MAGIC At inference time, scoring a user against all accounts is a single matrix multiply —
# MAGIC exactly like ALS — so both models have the same serving cost.
# MAGIC
# MAGIC ### ALS vs TF — why the hit rates are close
# MAGIC Both models are limited by the same thing: **data sparsity**.
# MAGIC With only 2–3 interactions per user on average, neither algorithm has much signal
# MAGIC to learn a meaningful preference profile.
# MAGIC
# MAGIC The difference between them is in *how* they handle it:
# MAGIC - **ALS** solves a least-squares problem algebraically — fast, interpretable, no gradient descent
# MAGIC - **TF embedding** uses gradient descent with negative sampling — more flexible, can incorporate
# MAGIC   side features (user demographics, content tags) in future iterations
# MAGIC
# MAGIC On a richer dataset (weeks of history, millions of interactions) the TF model typically
# MAGIC pulls ahead because it can learn non-linear patterns ALS cannot.
# MAGIC Here they're close — which is itself an interesting result.
# MAGIC
# MAGIC ### The champion / challenger gate
# MAGIC The notebook only promotes the TF model if `tf_hit_rate > als_hit_rate`.
# MAGIC This is the most basic form of a **deployment gate** — the same pattern used in
# MAGIC production ML pipelines: a challenger must definitively beat the champion before
# MAGIC it gets promoted. Nothing is deleted; both runs remain in MLflow with full lineage.
# MAGIC
# MAGIC ### Comparing runs in MLflow
# MAGIC Open **Experiments → mlops_social_recommender** in the left sidebar.
# MAGIC You will see two runs — one from Lab 1 (ALS), one from Lab 2 (TF).
# MAGIC Click **Compare** to see both `hit_rate_at_10` values side-by-side on the same chart.
# MAGIC This is the core MLflow workflow: every experiment is reproducible, comparable, and auditable.

# COMMAND ----------

winner = "TF embedding" if emb_hr > als_hr else "ALS"
margin = abs(emb_hr - als_hr)

print("=" * 55)
print("  Lab 2 — TF Embedding Recommender  |  Results")
print("=" * 55)
print(f"  Users            {N_USERS:>10,}")
print(f"  Accounts         {N_ACCOUNTS:>10,}")
print(f"  Embedding dim    {N_FACTORS:>10}  (N_FACTORS)")
print("-" * 55)
print(f"  ALS  Hit Rate@10 {als_hr:>10.4f}  (Lab 1 champion)")
print(f"  TF   Hit Rate@10 {emb_hr:>10.4f}  (Lab 2 challenger)")
print(f"  Margin           {margin:>10.4f}  → champion: {winner}")
print("-" * 55)
print(f"  MLflow run       {emb_run_id}")
print(f"  Model            {MODEL_NAME}")
print(f"  Registry status  {reg_status}")
print("=" * 55)
print()
print("Open Experiments → mlops_social_recommender to compare")
print("both runs side-by-side. Labs complete.")
