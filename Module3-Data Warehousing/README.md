# Module 3 Homework: Data Warehousing & BigQuery (Local Version)

This folder contains a local environment setup to work on the Module 3 Homework using **Postgres** and **Kestra**, without requiring a GCP/BigQuery account.

## 🚀 Setup Instructions

### 1. Start the Environment
```bash
docker-compose up -d
```

### 2. Access Kestra
Open [http://localhost:8080](http://localhost:8080) and complete the one-time setup (create an admin account).

### 3. Ingest Data (Flow 01)
1. Go to **Flows** → **Create**.
2. Paste the content of `flows/01_ingest_data.yaml` and **Save**.
3. Click **Execute** and wait for completion (green status).
   - Downloads Yellow Taxi 2024 Parquet files (Jan–Jun) and loads them into the `yellow_tripdata_2024` table.

### 4. Get Answers (Flow 03)
1. Go to **Flows** → **Create**.
2. Paste the content of `flows/03_homework_answers.yaml` and **Save**.
3. Click **Execute**.
4. Check the **Logs** tab for query results (Q1, Q2, Q4, Q6).

## Homework Answers

| Question | Answer |
|----------|--------|
| Q1 – Record count | *From Kestra logs* |
| Q2 – Estimated bytes | 0 MB for the External Table and 155.12 MB for the Materialized Table |
| Q3 – Columnar storage | BigQuery only scans requested columns; 2 columns = more bytes than 1 |
| Q4 – Zero fare trips | *From Kestra logs* |
| Q5 – Partitioning strategy | Partition by `tpep_dropoff_datetime` and Cluster on `VendorID` |
| Q6 – Partition benefits | 310.24 MB (non-partitioned) and 26.84 MB (partitioned) |
| Q7 – External table storage | GCP Bucket |
| Q8 – Always cluster? | False |
| Q9 – `count(*)` bytes | 0 bytes — BigQuery uses cached metadata for `count(*)` on materialized tables |

## 🛠️ Accessing DB Directly (Optional)
- **PgAdmin**: [http://localhost:8085](http://localhost:8085) — `admin@admin.com` / `root`
- **Postgres**: `localhost:5432` — `root` / `root`, DB: `ny_taxi`
