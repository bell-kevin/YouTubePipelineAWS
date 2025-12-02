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

# ---------------- Paths (adjust if needed) ----------------
COMMENTS_SENTIMENT_PATH = "s3://yt-analytics-cs6705-data/curated/comments_sentiment/"
TRENDING_PATH          = "s3://yt-analytics-cs6705-data/curated/trending/"
LABELED_FEATURES_PATH  = "s3://yt-analytics-cs6705-data/curated/labeled_features/"

# Allow dynamic partition overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ---------------- Load comments sentiment data ----------------
sent_df = spark.read.parquet(COMMENTS_SENTIMENT_PATH)

# sentiment schema already has ingest_date as DATE, so keep it
# if it's string in your version, cast it to date:
# sent_df = sent_df.withColumn("ingest_date", F.to_date("ingest_date"))

# ---------------- Load curated trending ----------------
trending_df = spark.read.parquet(TRENDING_PATH)

# IMPORTANT: trending_date is string in curated; make a DATE version for time windows
trending_df = (
    trending_df
        .withColumn("ingest_date", F.to_date("trending_date"))  # <-- key fix
)

# Basic sanity filters
trending_df = (
    trending_df
        .filter(F.col("video_id").isNotNull())
        .filter(F.col("region").isNotNull())
        .filter(F.col("ingest_date").isNotNull())
)

print("Trending row count:", trending_df.count())
trending_df.select("region", "ingest_date").groupBy("region").count().show()

# ---------------- Window for time-based features ----------------
w = Window.partitionBy("region", "video_id").orderBy("ingest_date")

feat_df = (
    trending_df
        .withColumn("prev_view_count",    F.lag("view_count").over(w))
        .withColumn("prev_like_count",    F.lag("like_count").over(w))
        .withColumn("prev_comment_count", F.lag("comment_count").over(w))
        .withColumn("prev_ingest_date",   F.lag("ingest_date").over(w))
        .withColumn("next_view_count",    F.lead("view_count").over(w))
        .withColumn("next_ingest_date",   F.lead("ingest_date").over(w))
)

# ---------------- Join sentiment features ----------------
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

# ---------------- Time delta ----------------
feat_df = feat_df.withColumn(
    "delta_days",
    F.when(
        F.col("prev_ingest_date").isNotNull(),
        F.datediff("ingest_date", "prev_ingest_date")
    ).otherwise(F.lit(1))
)

# ---------------- Velocities + ratios ----------------
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
        .withColumn("log_view_count",    F.log1p("view_count").cast("double"))
        .withColumn("log_like_count",    F.log1p("like_count").cast("double"))
        .withColumn("log_comment_count", F.log1p("comment_count").cast("double"))
)

# ---------------- Labels ----------------
feat_df = (
    feat_df
        .withColumn("log_next_view_count", F.log1p("next_view_count"))
        .withColumn(
            "log_view_growth_next",
            F.col("log_next_view_count") - F.col("log_view_count")
        )
        .withColumn(
            "stay_trending_next",
            # 1 if this video appears again at ANY later ingest_date in this region
            F.when(F.col("next_ingest_date").isNotNull(), F.lit(1.0)).otherwise(F.lit(0.0))
        )
)

feat_df = feat_df.withColumn(
    "stay_trending_next",
    F.col("stay_trending_next").cast("double")
)

labeled_df = feat_df

print("Backfill labeled rows:", labeled_df.count())
labeled_df.groupBy("stay_trending_next").count().show()

# ---------------- Write labeled features ----------------
(
    labeled_df
        .write
        .mode("overwrite")   # with dynamic partition mode, this overwrites partitions
        .partitionBy("region", "ingest_date")
        .parquet(LABELED_FEATURES_PATH)
)

print("Backfill complete ->", LABELED_FEATURES_PATH)
job.commit()
