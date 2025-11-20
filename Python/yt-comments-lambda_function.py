import json
import os
from datetime import datetime, timezone
import urllib.parse
import urllib.request
from urllib.error import HTTPError 

import boto3

secrets_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")
glue = boto3.client("glue")

YOUTUBE_API_KEY = None


def get_api_key():
    global YOUTUBE_API_KEY
    if YOUTUBE_API_KEY is not None:
        return YOUTUBE_API_KEY

    secret_name = "yt/youtube-api-key"
    resp = secrets_client.get_secret_value(SecretId=secret_name)
    secret_str = resp["SecretString"]
    data = json.loads(secret_str)
    YOUTUBE_API_KEY = data["YOUTUBE_API_KEY"]
    return YOUTUBE_API_KEY


def find_latest_trending_object(bucket, region, date_str):
    """
    Looks under raw/trending/region={region}/date={date_str}/
    and returns the key of the most recent JSON file.
    """
    prefix = f"raw/trending/region={region}/date={date_str}/"
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if "Contents" not in resp:
        raise RuntimeError(f"No trending objects found with prefix: {prefix}")

    # Keys contain timestamp, so lexicographically max is the newest
    keys = [obj["Key"] for obj in resp["Contents"]]
    keys.sort()
    return keys[-1]


def load_trending_video_ids(bucket, key, max_videos):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")
    data = json.loads(body)

    items = data.get("items", [])
    video_ids = []

    for item in items:
        # For videos.list with chart=mostPopular, id is the videoId string
        video_id = item.get("id")
        if video_id:
            video_ids.append(video_id)
        if len(video_ids) >= max_videos:
            break

    return video_ids


def fetch_comments_for_video(video_id, max_comments=50):
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
        # Log the error body so we can see the exact reason (comments disabled, quota, etc.)
        try:
            err_body = e.read().decode("utf-8")
        except Exception:
            err_body = "<no body>"

        print(f"HTTPError fetching comments for video {video_id}: {e.code} {e.reason}")
        print(f"Error body: {err_body}")
        # Just return an empty list for this video so the pipeline can continue
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
    bucket = os.environ["BUCKET_NAME"]
    regions_str = os.environ.get("REGIONS", "US")
    regions = [r.strip() for r in regions_str.split(",") if r.strip()]

    max_videos = int(os.environ.get("MAX_VIDEOS_PER_REGION", "20"))
    max_comments = int(os.environ.get("MAX_COMMENTS_PER_VIDEO", "50"))

    # Optionally make curated path configurable too:
    curated_comments_path = os.environ.get(
        "CURATED_COMMENTS_PATH",
        f"s3://{bucket}/curated/comments/"
    )

    now = datetime.utcnow()
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%Y%m%dT%H%M%SZ")

    results = {}
    glue_runs = []

    for region in regions:
        print(f"Processing comments for region={region}")

        # 1. Find latest trending file for this region + date
        trending_key = find_latest_trending_object(bucket, region, date_str)
        print(f"Using trending file: {trending_key}")

        # 2. Load video IDs
        video_ids = load_trending_video_ids(bucket, trending_key, max_videos)
        print(f"Found {len(video_ids)} video IDs")

        # 3. Fetch comments
        region_records = []
        for vid in video_ids:
            print(f"Fetching comments for video {vid}")
            comments = fetch_comments_for_video(vid, max_comments=max_comments)
            region_records.append({
                "videoId": vid,
                "comments": comments,
            })

        # 4. Write to S3
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

        results[region] = {
            "trending_key": trending_key,
            "comments_key": comments_key,
            "video_count": len(video_ids),
        }

        # Start Glue ETL job for this region + date
        raw_comments_path = f"s3://{bucket}/raw/comments/region={region}/date={date_str}"

        response = glue.start_job_run(
            JobName="yt_comments_etl_job",
            Arguments={
                "--RAW_S3_PATH": raw_comments_path,
                "--CURATED_S3_PATH": curated_comments_path,
            }
        )

        glue_runs.append({
            "region": region,
            "job_run_id": response["JobRunId"],
            "raw_path": raw_comments_path,
        })

        print(f"Started Glue job yt_comments_etl_job for region={region}, "
              f"JobRunId={response['JobRunId']}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Comments harvest complete",
            "results": results,
        }),
    }
