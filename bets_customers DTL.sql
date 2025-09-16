REATE OR REFRESH STREAMING TABLE mimic_gaming_data.customers_bronze.customers
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

-- core difference in Databricks vs postgress
-- you don't declare schema in the create table statement

--CREATE OR REFRESH STREAMING TABLE mimic_gaming_data.customers_bronze.customers(
--  customer_id STRING NOT NULL,
--  username STRING NOT NULL,

--FROM STREAM read_files(
 -- '/Volumes/.../files',
 -- format => 'json',
 -- schema => "customer_id STRING, ..."
--)

--	•	DLT tables get their schema either:
--	1.	Inferred from the source (read_files), or
--	2.	Explicitly given via schema => "..." inside read_files.



---Silver and some CASE and CAST functions

CREATE OR REFRESH STREAMING TABLE mimic_gaming_data.customers_silver.customers_in_silver
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
FROM STREAM mimic_gaming_data.customers_bronze.customers;


--simple MV for platers whi are under 1980 and with status platinum

CREATE OR REFRESH MATERIALIZED VIEW mimic_gaming_data.customers_silver.total_platinum_customers_under_1980
AS
SELECT COUNT(*) as total_platinum_customers_under_1980
FROM mimic_gaming_data.customers_silver.customers_in_silver as customers
WHERE vip_status = 'platinum'
AND date_of_birth < '1980-01-01'
