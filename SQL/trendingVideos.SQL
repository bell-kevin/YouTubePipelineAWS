WITH trending AS (
  SELECT
    region,
    date,
    i.id                    AS video_id,
    i.snippet.title         AS title,
    i.snippet.channelTitle  AS channel,
    i.snippet.publishedAt   AS video_published_at,
    CAST(i.statistics.viewCount  AS BIGINT) AS views,
    CAST(i.statistics.likeCount  AS BIGINT) AS video_likes
  FROM raw_trending
  CROSS JOIN UNNEST(items) AS t(i)
),

comments_flat AS (
  SELECT
    region,
    date,
    c.videoId            AS video_id,
    COUNT(*)             AS comment_count,
    AVG(c.likeCount)     AS avg_comment_likes
  FROM raw_comments
  CROSS JOIN UNNEST(comments) AS t(c)
  GROUP BY region, date, c.videoId
)

SELECT
  t.region,
  t.date,
  t.video_id,
  t.title,
  t.channel,
  t.video_published_at,
  t.views,
  t.video_likes,
  COALESCE(cf.comment_count, 0)      AS comment_count,
  COALESCE(cf.avg_comment_likes, 0)  AS avg_comment_likes
FROM trending t
LEFT JOIN comments_flat cf
  ON  t.region   = cf.region
  AND t.date     = cf.date
  AND t.video_id = cf.video_id
WHERE t.region = 'US'
  AND t.date   = '2025-11-19'
ORDER BY comment_count DESC
LIMIT 20;
