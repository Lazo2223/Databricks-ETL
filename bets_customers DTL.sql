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
