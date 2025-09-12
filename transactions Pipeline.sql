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
