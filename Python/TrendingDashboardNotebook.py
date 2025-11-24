from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import re

spark = SparkSession.builder.getOrCreate()

trending_path = "s3://yt-analytics-cs6705-data/curated/trending/"

trending_df = spark.read.parquet(trending_path)

trending_df.printSchema()

TOP_N = 20  # change as you like

top_videos_df = (
    trending_df
    .filter(F.col("view_count").isNotNull())
    .orderBy(F.col("view_count").desc())
    .select(
        "video_id",
        "title",
        "channel_id",
        "region",
        "trending_date",
        "view_count",
        "like_count",
        "comment_count"
    )
    .limit(TOP_N)
)

top_videos_df.show(truncate=False)

pdf_top = top_videos_df.toPandas()
pdf_top.head()



# Helper to strip emojis / non-ASCII so matplotlib's font doesn't choke
def strip_non_ascii(text):
    if text is None:
        return ""
    return re.sub(r'[^\x00-\x7F]+', '', text)

def plot_data(pdf_top, plot_title):
    pdf_top_plot = pdf_top.copy()

    # Build a shortened, cleaned title for x-axis labels
    pdf_top_plot["short_title"] = (
        pdf_top_plot["title"]
            .apply(strip_non_ascii)
            .str.slice(0, 40) + "..."
    )

    # Set default font size for all elements
    plt.rc('font', size=22) 

    plt.figure(figsize=(20, 15))
    plt.bar(pdf_top_plot["short_title"], pdf_top_plot["view_count"])
    plt.xticks(rotation=75, ha="right")
    plt.ylabel("View Count")
    plt.gcf().axes[0].yaxis.get_major_formatter().set_scientific(False)
    plt.title(plot_title)
    plt.tight_layout()

    # Add gridlines
    plt.grid(True) # Set to True to show gridlines

    # Customize gridline appearance (optional)
    plt.grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)

    # Add minor gridlines (optional)
    plt.minorticks_on()
    plt.grid(which='minor', linestyle=':', linewidth=0.2, alpha=0.5)
    return plt

def get_top_videos_by_region(region: str | None = None, top_n: int = 20):
    df = trending_df
    if region is not None:
        df = df.filter(F.col("region") == region)
    
    top_df = (
        df
        .filter(F.col("view_count").isNotNull())
        .orderBy(F.col("view_count").desc())
        .select(
            "video_id",
            "title",
            "channel_id",
            "region",
            "trending_date",
            "view_count",
            "like_count",
            "comment_count"
        )
        .limit(top_n)
    )
    return top_df.toPandas()

pdf_us = get_top_videos_by_region("US", 20)
pdf_us.head()
title = f"Top {len(pdf_top_plot)} Most Viewed Trending Videos in US"
plt = plot_data(pdf_us, title)
pdf_us = get_top_videos_by_region("CA", 20)
pdf_us.head()
plt = plot_data(pdf_us)
pdf_us = get_top_videos_by_region("GB", 20)
pdf_us.head()
plt = plot_data(pdf_us)
job.commit()