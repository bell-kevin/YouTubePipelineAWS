[Back to Readme](README.md)

# Project Proposal – YouTube Data Analytics Pipeline
### **Team:** Kevin Bell, Jake Child, Abel Mkina
### **Course:** CS 6705 – Applied Cloud Computing
### **Instructor:** Prof. Joshua Jensen Due Date: November 16, 2025
## 1. Project Overview
Our project will build a cloud-based YouTube analytics pipeline that retrieves trending-video metadata and comment text through the YouTube Data API v3, processes it on AWS, and performs descriptive and predictive analytics at scale using Apache Spark on EMR.

The system will:
* Collect daily “most popular” videos by region (U.S., Canada, UK) and append them to a historical trend dataset suitable for longitudinal analysis.
* Ingest high-volume comment data to determine sentiment (positive / neutral / negative) and surface emergent discussion topics for the top-trending videos.
* Engineer engagement features (view velocity, like-to-view ratios, subscriber deltas) and run distributed regression / classification models that forecast near-term view growth and probability of a video remaining in the trending list.
* Produce dashboards showing trending categories, geographic patterns, sentiment distributions, and predictive indicators derived from the Spark jobs.

Deliverables will include a working AWS pipeline, documented Spark notebooks / jobs that encapsulate the analytics, visual dashboards, and a final report describing architecture, data flows, models, and findings.
## 2. Tools and Technologies
* **Data Ingestion:** YouTube Data API v3, AWS Lambda (Python `googleapiclient`), Amazon EventBridge scheduler.
* **Storage & Catalog:** Amazon S3 data lake (raw JSON + curated Parquet), AWS Glue Data Catalog, AWS Lake Formation for governance.
* **Distributed Analytics:** Amazon EMR cluster (4–6 core nodes) running Apache Spark, PySpark, Spark MLlib, and Spark NLP for scalable sentiment and predictive modeling.
* **Interactive SQL:** Amazon Athena for ad-hoc exploration and validation of curated Parquet datasets.
* **Visualization:** Plotly Dash notebooks for rapid iteration.
* **Collaboration & CI/CD:** GitHub, Discord, Zoom, GitHub Actions for linting notebook jobs.
## 3. AWS Budget Estimate
Service | Expected Usage | Est. Monthly Cost (USD)
--- | --- | ---
Amazon S3 | 50 GB stored, 200K PUT/LIST | $3.00
AWS Lambda | 1M requests @ 512 MB, 5 min runtime | $6.50
AWS Glue (Crawler + Data Catalog) | Daily crawls + catalog storage | $8.00
Amazon Athena | 120 TB-scanned queries (compressed Parquet) | $4.50
Amazon EMR (Spark) | 1 master + 3 core `m5.xlarge` nodes for 45 hrs/mo (mix of on-demand + 50% spot) | $48.00
**Estimated Total** |  | **≈ $97.00**
## 4.   Weekly Milestones
Week | Milestone | Key Deliverables
---- | --------- | ----------------
Nov 3 – 9 | Team formation, API credential approval, historical data pull | API key verified, initial trending datasets profiled in notebook
Nov 10 – 16 | Updated proposal + ingestion design review | Revised proposal, sequence diagrams for ingestion and EMR orchestration
Nov 17 – 23 | Build AWS Lambda ingestion + S3 landing zone, configure Glue crawlers | Automated daily harvest pipeline, Glue tables for raw data
Nov 24 – 30 | Provision EMR cluster, develop Spark ETL (comment flattening, sentiment prep), persist curated Parquet | EMR bootstrap scripts, PySpark ETL jobs, curated datasets partitioned by region/date
Dec 1 – 7 | Run distributed analytics: sentiment scoring (Spark NLP), engagement feature engineering, gradient-boosted regression & classification models, topic modeling | Spark notebooks/jobs with evaluation metrics, model artifacts in S3, exploratory Athena queries
Dec 8 – 13 | Publish dashboards, conduct QA, finalize report & presentation | written analysis chapter, recorded demo
## 5. Team Collaboration Plan
* Communication: Discord channel + weekly Zoom check-ins.
* Version Control: GitHub repo (“yt-analytics-pipeline”).
* Task Tracking: Kanban board (GitHub Projects).
* AWS Access: Shared IAM roles with least-privilege policies.
## 6. Analytics Plan
* **Data Volume & Partitioning:**
  * Expect ≈200–250 trending videos per region per day (~20K records/mo) plus 2–3M associated comments.
  * Raw JSON will be partitioned by `region=REGION/date=YYYY-MM-DD` in S3.
  * Spark ETL will normalize nested comment threads into flat fact tables joined to video metadata.
* **Descriptive Analytics:**
  * Trending velocity: compute 1-day and 7-day rolling aggregates of views, likes, and comment volume per video using Spark window functions.
  * Category and geography insights: calculate per-category share-of-voice metrics and regionally normalized engagement scores, surfaced via Jupyter notebooks.
* **Sentiment & Topic Analysis:**
  * Spark NLP sentiment models fine-tuned on YouTube-specific corpora; aggregate distributions at video and region levels.
  * Latent Dirichlet Allocation (LDA) topic modeling on tokenized comments to surface trending discussion themes; results stored as topic-term matrices in Parquet.
* **Predictive Modeling:**
  * Feature engineering for engagement momentum (view acceleration, like velocity, sentiment polarity) with PySpark.
  * Gradient-boosted tree regression to forecast 24-hour view deltas; binary classification to predict whether a video remains trending next day. Metrics: RMSE, ROC-AUC, precision/recall.
  * Model governance: parameter sweeps logged via MLflow on EMR, with best models exported to S3 for reproducibility.
* **Operational Analytics:** Build automated Spark jobs that refresh aggregates daily and push summary tables to Athena.
## 7.   Expected Outcomes
By the end of the semester, the project will deliver:
* A fully automated YouTube data pipeline running on AWS.
* Dashboards illustrating regional and category-based trending patterns.
* Sentiment and prediction reports demonstrating applied cloud analytics.
