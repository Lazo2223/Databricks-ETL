-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE mimic_gaming_data.bets_history_bronze.raw_transactions_1
AS
SELECT
customer_id,
bet_id,
bet_type,
timestamp_bet,
timestamp_settlement,
event_name,
casino_game_name,
game_provider,
stake_eur,
gains_eur,
is_free_bet,
channel,
currency
FROM STREAM read_files(
  '/Volumes/mimic_gaming_data/bets_history_bronze/files',
  format => 'json',
  schema => "customer_id STRING, bet_id STRING, bet_type STRING, timestamp_bet STRING, timestamp_settlement STRING, event_name STRING, casino_game_name STRING, game_provider STRING, stake_eur DOUBLE, gains_eur DOUBLE, is_free_bet BOOLEAN, channel STRING, currency STRING" 
)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE mimic_gaming_data.bets_history_silver.transactions_history_1
AS
SELECT
customer_id,
bet_id,
bet_type,
timestamp_bet,
timestamp_settlement,
event_name,
casino_game_name,
game_provider,
stake_eur,
CAST(stake_eur / 1.17 as DOUBLE) as stake_usd,
gains_eur,
TRY_CAST(gains_eur / 1.17 as DOUBLE) as gains_usd,
CASE WHEN gains_eur = 0 THEN "Lost" ELSE "Win" END as win_loss,
is_free_bet,
channel,
currency
FROM STREAM mimic_gaming_data.bets_history_bronze.raw_transactions_1

-- COMMAND ----------

--Now players

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE mimic_gaming_data.customers_bronze.customers_1
AS
SELECT 
  customer_id,
  username,
  first_name,
  last_name,
  date_of_birth,
  place_of_birth,
  country,
  state_province,
  signup_date,
  vip_status,
  self_excluded,
  preferred_channel,
  current_timestamp() AS ingest_timestamp,
  _metadata.file_name AS file_name
FROM STREAM read_files(
  '/Volumes/mimic_gaming_data/customers_bronze/files',
  format => 'json',
  schema => "customer_id STRING, username STRING, first_name STRING, last_name STRING, date_of_birth STRING, place_of_birth STRING, country STRING, state_province STRING, signup_date STRING, vip_status STRING, self_excluded BOOLEAN, preferred_channel STRING"
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE mimic_gaming_data.customers_silver.customers_in_silver_1
AS
SELECT
  customer_id,
  username,
  first_name,
  last_name,
  TRY_CAST(date_of_birth AS DATE) AS date_of_birth,
  CASE
  WHEN date_of_birth IS NULL THEN NULL
  WHEN date_of_birth < '1939-01-01' THEN "Old Player"
  WHEN date_of_birth > '2000-01-01' THEN "Young Player"
  ELSE date_of_birth
  END AS date_of_birth_checked,
  place_of_birth,
  CASE
  WHEN place_of_birth = 'United Kingdom' THEN "NonEU"
  ELSE place_of_birth
  END AS is_UK_citizen,
  country,
  state_province,
  TRY_CAST(signup_date AS DATE) AS signup_date,
  vip_status,
  TRY_CAST(self_excluded AS BOOLEAN) AS self_excluded,
  preferred_channel,
  ingest_timestamp,
  file_name
FROM STREAM mimic_gaming_data.customers_bronze.customers_1;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW mimic_gaming_data.customers_silver.total_platinum_customers_under_1980_1
AS
SELECT COUNT(*) as total_platinum_customers_under_1980
FROM mimic_gaming_data.customers_silver.customers_in_silver_1 as customers
WHERE vip_status = 'platinum'
AND date_of_birth < '1980-01-01'

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW mimic_gaming_data.bets_history_gold.transactions_after_2023_1 AS
SELECT cs.customer_id
FROM mimic_gaming_data.customers_silver.customers_in_silver_1 as cs
LEFT JOIN mimic_gaming_data.bets_history_silver.transactions_history_1 th
ON cs.customer_id = th.customer_id
WHERE year(signup_date) >= '2023'

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW mimic_gaming_data.bets_history_gold.transactions_enriched_customers
AS
SELECT *
EXCEPT(th.customer_id)
FROM mimic_gaming_data.customers_silver.customers_in_silver_1 as cs
JOIN mimic_gaming_data.bets_history_silver.transactions_history_1 th
ON cs.customer_id = th.customer_id

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW mimic_gaming_data.bets_history_gold.profit_from_channel
AS
SELECT channel, round(SUM(stake_eur) - SUM(gains_eur), 2) as profit
FROM mimic_gaming_data.bets_history_gold.transactions_enriched_customers
WHERE channel IN ('mobile_app', 'retail_shop','web_browser') 
GROUP BY channel
