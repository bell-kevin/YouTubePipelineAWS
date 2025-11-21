import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------- Glue boilerplate ----------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------- Paths ----------------
COMMENTS_SENTIMENT_PATH = "s3://yt-analytics-cs6705-data/curated/comments_sentiment/"
TRENDING_PATH          = "s3://yt-analytics-cs6705-data/curated/trending/"
LABELED_FEATURES_PATH  = "s3://yt-analytics-cs6705-data/curated/labeled_features/"

# ---------------- Load comments sentiment data ----------------
sent_df = spark.read.parquet(COMMENTS_SENTIMENT_PATH)
# make sure ingest_date is a DATE
sent_df = sent_df.withColumn("ingest_date", F.to_date("ingest_date"))

# ---------------- Load curated trending ----------------
trending_df = (
    spark.read.parquet(TRENDING_PATH)
    .withColumn("ingest_date", F.to_date("trending_date"))
)

# Window for time-based features
w = Window.partitionBy("region", "video_id").orderBy("ingest_date")

# 1) Add prev/next metrics  **(ONLY HERE do we start feat_df)**
feat_df = (
    trending_df
        .withColumn("prev_view_count",    F.lag("view_count").over(w))
        .withColumn("prev_like_count",    F.lag("like_count").over(w))
        .withColumn("prev_comment_count", F.lag("comment_count").over(w))
        .withColumn("prev_ingest_date",   F.lag("ingest_date").over(w))
        .withColumn("next_view_count",    F.lead("view_count").over(w))
        .withColumn("next_ingest_date",   F.lead("ingest_date").over(w))
)

# 🔴 DO **NOT** do: feat_df = trending_df.join(...)
#    That’s what erases prev_view_count and causes your error.

# 2) Join sentiment onto THIS feat_df
feat_df = (
    feat_df
        .join(
            sent_df,
            on=["video_id", "region", "ingest_date"],
            how="left"
        )
        .fillna({
            "avg_comment_sent_pos": 0.0,
            "avg_comment_sent_neg": 0.0,
            "avg_comment_sent_neu": 0.0,
            "avg_comment_sent_mix": 0.0,
            "net_comment_sentiment": 0.0,
            "comment_count_scored": 0
        })
)

# 3) Time delta
feat_df = feat_df.withColumn(
    "delta_days",
    F.when(
        F.col("prev_ingest_date").isNotNull(),
        F.datediff("ingest_date", "prev_ingest_date")
    ).otherwise(F.lit(1))
)

# 💡 Sanity check right here:
feat_df.printSchema()
# You should see: prev_view_count, prev_like_count, prev_comment_count, prev_ingest_date, delta_days, etc.

# 4) Velocities + ratios (now prev_* definitely exists)
feat_df = (
    feat_df
        .withColumn(
            "view_velocity",
            ((F.col("view_count") - F.col("prev_view_count")) / F.col("delta_days")).cast("double")
        )
        .withColumn(
            "like_velocity",
            (F.col("like_count") - F.col("prev_like_count")).cast("double")
        )
        .withColumn(
            "comment_velocity",
            (F.col("comment_count") - F.col("prev_comment_count")).cast("double")
        )
        .withColumn(
            "view_growth_ratio",
            (F.col("view_count") / (F.col("prev_view_count") + F.lit(1.0))).cast("double")
        )
        .withColumn(
            "like_growth_ratio",
            (F.col("like_count") / (F.col("prev_like_count") + F.lit(1.0))).cast("double")
        )
        .withColumn(
            "comment_growth_ratio",
            (F.col("comment_count") / (F.col("prev_comment_count") + F.lit(1.0))).cast("double")
        )
        .withColumn(
            "like_view_ratio",
            (F.col("like_count") / (F.col("view_count") + F.lit(1.0))).cast("double")
        )
        .withColumn(
            "comment_view_ratio",
            (F.col("comment_count") / (F.col("view_count") + F.lit(1.0))).cast("double")
        )
        .withColumn(
            "engagement_per_view",
            ((F.col("like_count") + F.col("comment_count")) / (F.col("view_count") + F.lit(1.0))).cast("double")
        )
        .withColumn(
            "video_age_days",
            F.datediff("ingest_date", F.to_date("published_at")).cast("double")
        )
        .withColumn("log_view_count", F.log1p("view_count").cast("double"))
        .withColumn("log_like_count", F.log1p("like_count").cast("double"))
        .withColumn("log_comment_count", F.log1p("comment_count").cast("double"))
)


# 5) Labels
feat_df = (
    feat_df
        .withColumn("log_next_view_count", F.log1p("next_view_count"))
        .withColumn(
            "log_view_growth_next",
            F.col("log_next_view_count") - F.col("log_view_count")
        )
        .withColumn(
            "stay_trending_next",
            F.when(F.col("next_view_count").isNotNull(), F.lit(1)).otherwise(F.lit(0))
        )
)

# 6) Drop rows without next snapshot
labeled_df = feat_df.filter(F.col("next_view_count").isNotNull())

print("Labeled count:", labeled_df.count())
labeled_df.show(5)

# 7) Write out
(
    labeled_df
        .write
        .mode("append")
        .partitionBy("region", "ingest_date")
        .parquet(LABELED_FEATURES_PATH)
)

job.commit()
