CREATE OR REFRESH STREAMING TABLE bi_projects.bronze_cars.raw_cars_data
AS 
SELECT *,
  current_timestamp() AS ingestion_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/bi_projects/bronze_cars/files',
FORMAT => 'json'
);

--todo

CREATE OR REFRESH STREAMING TABLE bi_projects.silver_cars.flat_data
AS 
SELECT 
TRY_CAST(Customer_Age AS INT) customer_age,
Customer_ID,
Customer_Loyalty_Tier,
Device_Type,
Footfall_Count,
Fraud_Flag,
IP_Address,
Location,
Payment_Method,
Product_Category,
Product_SKU
Purchase_Amount
Store_ID
split(Store_ID, '-')[1] AS store_city
Transaction_Date
Transaction_ID
Transaction_Time
_rescued_data
ingestion_time
source_file
FROM STREAM bi_projects.bronze_cars.raw_cars_data
