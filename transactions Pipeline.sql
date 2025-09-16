CREATE OR REFRESH STREAMING TABLE mimic_gaming_data.bets_history_bronze.raw_transactions
AS
SELECT
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
  schema => "bet_id STRING, bet_type STRING, timestamp_bet STRING, timestamp_settlement STRING, event_name STRING, casino_game_name STRING, game_provider STRING, stake_eur DOUBLE, gains_eur DOUBLE, is_free_bet BOOLEAN, channel STRING, currency STRING" 
)






CREATE OR REFRESH STREAMING TABLE mimic_gaming_data.bets_history_silver.transactions_history
AS
SELECT
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
FROM STREAM mimic_gaming_data.bets_history_bronze.raw_transactions



--Simple MV for players, join with transaction_history

CREATE OR REFRESH MATERIALIZED VIEW mimic_gaming_data.bets_history_gold.transactions_after_2023 AS
SELECT cs.customer_id
FROM mimic_gaming_data.customers_silver.customers_in_silver as cs
LEFT JOIN mimic_gaming_data.bets_history_silver.transactions_history th
ON cs.customer_id = th.customer_id
WHERE year(signup_date) >= '2023'
