CREATE OR REFRESH STREAMING TABLE mimic_gaming_data.customers_bronze.customers_bronze_clean
(
CONSTRAINT valid_cid EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE, 
CONSTRAINT valid_name EXPECT (username IS NOT NULL),
CONSTRAINT valid_address EXPECT (country IS NOT NULL and place_of_birth IS NOT NULL and state_province IS NOT NULL),
CONSTRAINT valid_SE EXPECT (self_excluded IS NOT NULL) ON VIOLATION DROP ROW
)

AS
SELECT
*
FROM STREAM mimic_gaming_data.customers_bronze.customers

--

CREATE OR REFRESH STREAMING TABLE mimic_gaming_data.customers_silver.customer_changes
COMMENT "New customer Data";

CREATE FLOW changes_flow AS
AUTO CDC INTO mimic_gaming_data.customers_silver.customer_changes
FROM STREAM mimic_gaming_data.customers_bronze.customers_bronze_clean
KEYS (customer_id)


---example for MV

CREATE OR REFRESH MATERIALIZED VIEW 3_gold_db. current_customers_gold_demo
COMMENT "Current updated list of active customers"
* EXCEPT (processing_time), current_timestamp() updated_at
FROM 2_silver_db. customers_silver_demo
WHERE 'END_AT IS NULL;
the current version of the record
-- Filter for only rows that contain a null value for __END_AT, which indicates
