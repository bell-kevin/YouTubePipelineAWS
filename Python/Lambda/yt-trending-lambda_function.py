import json
import os
from datetime import datetime, timezone
import urllib.parse
import urllib.request
from urllib.error import HTTPError

import boto3

# AWS clients
secrets_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")
glue = boto3.client("glue")

# Cache the key across invocations
YOUTUBE_API_KEY = None


def get_api_key():
    """
    Retrieve and cache YouTube API key from Secrets Manager.
    Secret must contain: {"YOUTUBE_API_KEY": "<key>"}.
    """
    global YOUTUBE_API_KEY
    if YOUTUBE_API_KEY is not None:
        return YOUTUBE_API_KEY

    secret_name = "yt/youtube-api-key"
    resp = secrets_client.get_secret_value(SecretId=secret_name)
    secret_str = resp["SecretString"]
    data = json.loads(secret_str)
    YOUTUBE_API_KEY = data["YOUTUBE_API_KEY"]
    return YOUTUBE_API_KEY


def fetch_trending(region_code, max_results=50):
    """
    Call videos.list with chart=mostPopular to get trending videos for a region.
    """
    api_key = get_api_key()

    base_url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "part": "snippet,statistics,contentDetails",
        "chart": "mostPopular",
        "regionCode": region_code,
        "maxResults": max_results,
        "key": api_key,
    }
    url = f"{base_url}?{urllib.parse.urlencode(params)}"

    with urllib.request.urlopen(url) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body)


def fetch_comments_for_video(video_id, max_comments=250):
    """
    Call commentThreads.list for a single video, returning a list of comment dicts.
    """
    api_key = get_api_key()
    base_url = "https://www.googleapis.com/youtube/v3/commentThreads"

    comments = []
    params = {
        "part": "snippet",
        "videoId": video_id,
        "maxResults": min(max_comments, 100),
        "textFormat": "plainText",
        "order": "relevance",
        "key": api_key,
    }

    url = f"{base_url}?{urllib.parse.urlencode(params)}"

    try:
        with urllib.request.urlopen(url) as resp:
            body = resp.read().decode("utf-8")
            data = json.loads(body)
    except HTTPError as e:
        # Log error body so we can see if comments are disabled, quota exceeded, etc.
        try:
            err_body = e.read().decode("utf-8")
        except Exception:
            err_body = "<no body>"
        print(f"HTTPError fetching comments for video {video_id}: {e.code} {e.reason}")
        print(f"Error body: {err_body}")
        # Return empty list so the pipeline continues
        return []

    for item in data.get("items", []):
        top = item.get("snippet", {}).get("topLevelComment", {}).get("snippet", {})
        comment = {
            "videoId": video_id,
            "authorDisplayName": top.get("authorDisplayName"),
            "textDisplay": top.get("textDisplay"),
            "publishedAt": top.get("publishedAt"),
            "likeCount": top.get("likeCount"),
        }
        comments.append(comment)
        if len(comments) >= max_comments:
            break

    return comments


def lambda_handler(event, context):
    # Environment variables
    bucket = os.environ["BUCKET_NAME"]  # e.g. yt-analytics-cs6705-data
    regions_str = os.environ.get("REGIONS", "US")
    regions = [r.strip() for r in regions_str.split(",") if r.strip()]

    max_videos = int(os.environ.get("MAX_VIDEOS_PER_REGION", "20"))
    max_comments = int(os.environ.get("MAX_COMMENTS_PER_VIDEO", "50"))

    curated_path = os.environ.get(
        "CURATED_PATH",
        f"s3://{bucket}/curated/",
    )

    # Current UTC date/time
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%Y%m%dT%H%M%SZ")

    results = {}
    glue_runs = []

    for region in regions:
        print(f"=== Processing region={region} ===")

        # 1) Fetch trending videos from YouTube API
        print(f"Fetching trending videos for region={region}")
        trending_data = fetch_trending(region, max_results=max_videos)

        items = trending_data.get("items", [])
        print(f"Trending returned {len(items)} items")

        # Extract video IDs (up to max_videos)
        video_ids = []
        for item in items:
            vid = item.get("id")
            if vid:
                video_ids.append(vid)
            if len(video_ids) >= max_videos:
                break

        print(f"Using {len(video_ids)} video IDs for comments")

        # 2) Write raw trending JSON to S3
        trending_key = (
            f"raw/trending/region={region}/date={date_str}/"
            f"trending_{region}_{ts_str}.json"
        )

        s3_client.put_object(
            Bucket=bucket,
            Key=trending_key,
            Body=json.dumps(trending_data).encode("utf-8"),
            ContentType="application/json",
        )

        print(f"Wrote trending data to s3://{bucket}/{trending_key}")

        # 3) Fetch comments for each video
        region_records = []
        total_comments = 0

        for vid in video_ids:
            print(f"Fetching comments for video {vid}")
            comments = fetch_comments_for_video(vid, max_comments=max_comments)
            total_comments += len(comments)

            region_records.append({
                "videoId": vid,
                "comments": comments,
            })

        # 4) Write raw comments JSON to S3
        comments_key = (
            f"raw/comments/region={region}/date={date_str}/"
            f"comments_{region}_{ts_str}.json"
        )

        s3_client.put_object(
            Bucket=bucket,
            Key=comments_key,
            Body=json.dumps(region_records).encode("utf-8"),
            ContentType="application/json",
        )

        print(f"Wrote comments data to s3://{bucket}/{comments_key}")
        print(f"Total comments fetched for region={region}: {total_comments}")


        # Record result summary for this region
        results[region] = {
            "trending_s3_key": trending_key,
            "trending_item_count": len(items),
            "comments_s3_key": comments_key,
            "video_count": len(video_ids),
            "total_comments": total_comments,
        }

    # 5) Start Glue ETL workflow for all regions
    raw_trending_path = f"s3://{bucket}/raw/"

    response = glue.start_workflow_run(
        Name="TrendingWorkflow",
        RunProperties={
            "RAW_S3_PATH": raw_trending_path,
            "CURATED_S3_PATH": curated_path,
        }
    )

    glue_runs.append({
        "region": region,
        "workflow_run_id": response["RunId"],
        "raw_path": raw_trending_path,
    })

    print(
        f"Started Glue workflow TrendingWorkflow for region={region}, "
        f"WorkflowRunId={response['RunId']}"
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Trending + comments harvest complete, Glue workflows triggered",
            "results": results,
            "glue_runs": glue_runs,
        }),
    }
