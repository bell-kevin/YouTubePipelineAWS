import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

# ---------------- Glue boilerplate ----------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------- Config: paths ----------------
# These match what you're already using in your other jobs
FEATURES_PATH = "s3://yt-analytics-cs6705-data/curated/labeled_features/"
REG_MODEL_PATH = "s3://yt-analytics-cs6705-data/models/gbt_reg_view_growth"
CLS_MODEL_PATH = "s3://yt-analytics-cs6705-data/models/logreg_stay_trending"
PREDICTIONS_PATH = "s3://yt-analytics-cs6705-data/predictions/trending/"

# ---------------- 1) Load feature data ----------------
# This is the output from FeatureAndLabelEngineering.py
feat_df = spark.read.parquet(FEATURES_PATH)

# We assume FeatureAndLabelEngineering created at least these:
# - video_id, channel_id, region, ingest_date, trending_date
# - view_count, like_count, comment_count
# - log_view_count, log_like_count, log_comment_count
# - feature columns used for training
# - target columns (log_view_growth_next, stay_trending_next, etc.)

# ---------------- 2) Select latest ingest_date for scoring ----------------
# We only score the most recent snapshot of trending videos.
max_ingest_date = feat_df.agg(F.max("ingest_date")).collect()[0][0]

scoring_df = feat_df.filter(F.col("ingest_date") == F.lit(max_ingest_date))

# (Optional sanity print)
print("Scoring ingest_date:", max_ingest_date)
print("Scoring row count:", scoring_df.count())

# ---------------- 3) Load trained models ----------------
# These are PipelineModels that include StringIndexer + VectorAssembler + Model
reg_model = PipelineModel.load(REG_MODEL_PATH)
cls_model = PipelineModel.load(CLS_MODEL_PATH)

# ---------------- 4) Apply regression model ----------------
# Drop any columns that the pipeline will create itself
for c in ["region_idx", "features"]:
    if c in scoring_df.columns:
        scoring_df = scoring_df.drop(c)

# Predict log_view_growth_next (as we trained it)
reg_scored = reg_model.transform(scoring_df)

# Rename regression prediction column so it doesn't conflict with classifier
reg_scored = reg_scored.withColumnRenamed(
    "prediction",
    "pred_log_view_growth_next"
)

# Using the growth prediction plus current log_view_count
# we can estimate next day's log_view_count and view_count.

# NOTE: This assumes your label was defined as:
#   log_view_growth_next = log1p(next_view_count) - log1p(view_count)
# So: next_log_view_count ≈ log_view_count + pred_log_view_growth_next
reg_scored = (
    reg_scored
    .withColumn(
        "pred_log_view_count_next",
        F.col("log_view_count") + F.col("pred_log_view_growth_next")
    )
    .withColumn(
        "pred_view_count_next",
        F.expm1(F.col("pred_log_view_count_next"))
    )
)

# ---------------- 5) Apply classification model ----------------
# Predict whether the video will stay trending next day

# IMPORTANT: reg_model's pipeline already created region_idx and features.
# cls_model's pipeline ALSO tries to create them, so we must drop them first.
cls_input = reg_scored
for c in ["region_idx", "features"]:
    if c in cls_input.columns:
        cls_input = cls_input.drop(c)

cls_scored = cls_model.transform(cls_input)

# Rename prediction only, keep probability as a vector column
cls_scored = cls_scored.withColumnRenamed(
    "prediction", "pred_stay_trending_next"
)

# Extract the probability of class "1" (stays trending) from the vector
cls_scored = cls_scored.withColumn(
    "prob_stay_trending_next",
    vector_to_array("probability").getItem(1)
)

# ---------------- 6) Select prediction output schema ----------------
# Choose the columns you want to expose downstream (Athena/QuickSight)
output_cols = [
    "video_id",
    "channel_id",
    "region",
    "ingest_date",
    "trending_date",
    "title",
    "view_count",
    "like_count",
    "comment_count",
    "log_view_count",
    "log_like_count",
    "log_comment_count",
    # Predictions
    "pred_log_view_growth_next",
    "pred_log_view_count_next",
    "pred_view_count_next",
    "pred_stay_trending_next",
    "prob_stay_trending_next",
]

# Only keep columns that actually exist (to be resilient to minor schema changes)
existing_cols = [c for c in output_cols if c in cls_scored.columns]
predictions_df = cls_scored.select(*existing_cols)

# ---------------- 7) Write predictions to S3 ----------------
(
    predictions_df
    .write
    .mode("overwrite")
    .partitionBy("region", "ingest_date")
    .parquet(PREDICTIONS_PATH)
)

print("Wrote predictions to:", PREDICTIONS_PATH)

job.commit()
