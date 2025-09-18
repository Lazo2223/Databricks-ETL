# Databricks-ETL-Playground

This project is a sandbox for experimenting with Databricks ETL patterns using **Streaming Tables**, **Materialized Views**, and **Jobs orchestration**.  
The goal is to understand the fundamentals of building data pipelines in the **medallion architecture** (Bronze → Silver → Gold).

---

## Key Learnings

- **Catalogs** → logical containers for organizing schemas and tables.  
- **Delta Tables** → Databricks’ default storage format that supports ACID transactions and streaming.  
- **Managed Tables** → tables whose lifecycle and storage are managed by Databricks.  
- **Streaming ingestion** → simulating Kafka-like streams by continuously reading JSON files.  
- **JSON Flattening** → extracting nested JSON fields into structured columns.  
- **Medallion Architecture**:
  - **Bronze** → raw ingestion from files.  
  - **Silver** → data cleaned, transformed, and standardized.  
  - **Gold** → business-level aggregates and metrics.  

---

## ETL Flow (Simplified)

1. **Bronze Layer**  
   - Ingests raw `transactions` and `customers` data from JSON files using `read_files()`.  

2. **Silver Layer**  
   - Adds transformations like casting, enrichment (e.g., converting EUR → USD), and business logic (Win/Loss, VIP checks, citizenship logic).  

3. **Gold Layer**  
   - Builds **Materialized Views** to deliver KPIs and ready-to-use datasets, such as:  
     - Platinum customers born before 1980.  
     - Transactions filtered by signup year.  
     - Enriched transactions joined with customers.  
     - Profit by sales channel.  

---

## Example Code Snippet

```sql
-- Bronze: raw transactions ingestion
CREATE OR REFRESH STREAMING TABLE mimic_gaming_data.bets_history_bronze.raw_transactions_1 AS
SELECT *
FROM STREAM read_files(
  '/Volumes/mimic_gaming_data/bets_history_bronze/files',
  format => 'json',
  schema => "customer_id STRING, bet_id STRING, bet_type STRING, stake_eur DOUBLE, gains_eur DOUBLE"
);

-- Silver: transactions with enrichment
CREATE OR REFRESH STREAMING TABLE mimic_gaming_data.bets_history_silver.transactions_history_1 AS
SELECT
  customer_id,
  stake_eur,
  CAST(stake_eur / 1.17 AS DOUBLE) AS stake_usd,
  CASE WHEN gains_eur = 0 THEN 'Lost' ELSE 'Win' END AS win_loss
FROM STREAM mimic_gaming_data.bets_history_bronze.raw_transactions_1;
