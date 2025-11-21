import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from pyspark.ml.feature import VectorAssembler, StringIndexer, Imputer
from pyspark.ml import Pipeline
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import RegressionEvaluator, BinaryClassificationEvaluator

# -------- Glue / Spark setup --------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------- Load labeled dataset --------
LABELED_FEATURES_PATH = "s3://yt-analytics-cs6705-data/curated/labeled_features/"

labeled_df = spark.read.parquet(LABELED_FEATURES_PATH)

# Keep only rows that come from the *new* feature job (have sentiment columns populated)
labeled_df = labeled_df.filter(F.col("avg_comment_sent_pos").isNotNull())

# -------- Feature columns / indexers --------

# NOTE: We REMOVE velocity/growth columns because they are all NULL in the current data
numeric_feature_cols = [
    "log_view_count",
    "log_like_count",
    "log_comment_count",
    # removed: view_velocity, like_velocity, comment_velocity
    # removed: view_growth_ratio, like_growth_ratio, comment_growth_ratio
    "like_view_ratio",
    "comment_view_ratio",
    "engagement_per_view",
    "video_age_days",
    # Sentiment features
    "avg_comment_sent_pos",
    "avg_comment_sent_neg",
    "avg_comment_sent_neu",
    "avg_comment_sent_mix",
    "net_comment_sentiment",
    "comment_count_scored",
]

# Label column for regression
label_col = "log_view_growth_next"

# Categorical indexers
indexers = [
    StringIndexer(inputCol="region", outputCol="region_idx", handleInvalid="keep")
    # StringIndexer(inputCol="category_id", outputCol="category_idx", handleInvalid="keep")
]

feature_cols_with_cat = numeric_feature_cols + ["region_idx"]  # + ["category_idx"] if used

# -------- Clean types & handle Inf/NaN --------

# Ensure all numeric feature cols are double
for c in numeric_feature_cols:
    if c in labeled_df.columns:
        labeled_df = labeled_df.withColumn(c, F.col(c).cast("double"))

# Also make sure label is double
labeled_df = labeled_df.withColumn(label_col, F.col(label_col).cast("double"))

# Convert +/-Inf to NULL in numeric features + label
cols_to_clean = numeric_feature_cols + [label_col]
for c in cols_to_clean:
    if c in labeled_df.columns:
        labeled_df = labeled_df.withColumn(
            c,
            F.when(
                (F.col(c) == float("inf")) | (F.col(c) == float("-inf")),
                F.lit(None)
            ).otherwise(F.col(c))
        )

# Drop rows with bad / missing label
clean_df = labeled_df.filter(F.col(label_col).isNotNull())

print("Total rows after label cleaning:", clean_df.count())

# -------- Train / test split (time-aware) --------

distinct_days = clean_df.select("ingest_date").distinct().count()

if distinct_days <= 3:
    print(
        f"Only {distinct_days} distinct ingest_date values; "
        "using randomSplit instead of time-based split."
    )
    train_df, test_df = clean_df.randomSplit([0.8, 0.2], seed=42)
else:
    cutoff_date = clean_df.agg(F.max("ingest_date")).collect()[0][0]
    test_start = F.date_sub(F.lit(cutoff_date), 3)  # last 3 days as test
    print("cutoff_date:", cutoff_date, "test_start:", test_start)

    train_df = clean_df.filter(F.col("ingest_date") < test_start)
    test_df = clean_df.filter(F.col("ingest_date") >= test_start)

print("Train rows:", train_df.count())
print("Test rows:", test_df.count())

# -------- Imputer & assembler --------

imputer = Imputer(
    inputCols=numeric_feature_cols,
    outputCols=numeric_feature_cols,  # overwrite in place
    strategy="median"
)

assembler = VectorAssembler(
    inputCols=feature_cols_with_cat,
    outputCol="features",
    handleInvalid="keep"
)

# ---- Regression: predict log_view_growth_next ----
regressor = GBTRegressor(
    labelCol=label_col,
    featuresCol="features",
    maxIter=50,
    maxDepth=5
)

reg_pipeline = Pipeline(stages=indexers + [imputer, assembler, regressor])

# ---- Classification: predict stay_trending_next ----
classifier = LogisticRegression(
    labelCol="stay_trending_next",
    featuresCol="features",
    maxIter=50
)

cls_pipeline = Pipeline(stages=indexers + [imputer, assembler, classifier])

# -------- Train models --------
reg_model = reg_pipeline.fit(train_df)
cls_model = cls_pipeline.fit(train_df)

# -------- Evaluate regression --------
reg_preds = reg_model.transform(test_df)
reg_eval = RegressionEvaluator(
    labelCol=label_col,
    predictionCol="prediction",
    metricName="rmse"
)
rmse = reg_eval.evaluate(reg_preds)
print("Regression RMSE (log_view_growth_next):", rmse)

# -------- Evaluate classification --------
cls_preds = cls_model.transform(test_df)
cls_eval = BinaryClassificationEvaluator(
    labelCol="stay_trending_next",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)
auc = cls_eval.evaluate(cls_preds)
print("Classification AUC (stay_trending_next):", auc)

# -------- Save models --------
reg_model.write().overwrite().save("s3://yt-analytics-cs6705-data/models/gbt_reg_view_growth")
cls_model.write().overwrite().save("s3://yt-analytics-cs6705-data/models/logreg_stay_trending")

job.commit()
