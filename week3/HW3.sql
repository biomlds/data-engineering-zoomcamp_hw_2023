-- Setup
-- Creating external table using the fhv 2019 data referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `fhv_tripdata.2019_external_table`
OPTIONS (
  format = 'CSV',
  uris = ['gs://week3_fhv_tripdata/data/fhv_tripdata/fhv_tripdata_2019-*.csv.gz']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE fhv_tripdata.2019_table_non_partitoned AS
SELECT * FROM fhv_tripdata.2019_external_table;


--- Q1. What is the count for fhv vehicle records for year 2019?
SELECT COUNT(1) FROM fhv_tripdata.2019_table_non_partitoned;


-- Q2. Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
SELECT DISTINCT affiliated_base_number
FROM fhv_tripdata.2019_external_table;

SELECT DISTINCT affiliated_base_number
FROM fhv_tripdata.2019_table_non_partitoned;


-- Q3. How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
SELECT COUNT(1)
FROM fhv_tripdata.2019_table_non_partitoned
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;

-- Q4. What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

/*  Partition by affiliated_base_number Cluster on pickup_datetime

CREATE OR REPLACE TABLE fhv_tripdata.2019_table_non_partitoned_clustered2x
PARTITION BY (affiliated_base_number)
CLUSTER BY DATE(pickup_datetime) AS
SELECT * FROM fhv_tripdata.2019_table_non_partitoned; 
*/



-- Q5. Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive). Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.
CREATE OR REPLACE TABLE fhv_tripdata.2019_table_non_partitoned_clustered_part_clust
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM fhv_tripdata.2019_table_non_partitoned;

-- 647 MB
SELECT DISTINCT affiliated_base_number
FROM fhv_tripdata.2019_table_non_partitoned
WHERE DATE(pickup_datetime) BETWEEN DATE('2019-03-01') AND DATE('2019-03-31');

-- 23 MB
SELECT DISTINCT affiliated_base_number
FROM fhv_tripdata.2019_table_non_partitoned_clustered_part_clust
WHERE DATE(pickup_datetime) BETWEEN DATE('2019-03-01') AND DATE('2019-03-31');

 -- Q6. Where is the data stored in the External Table you created?
 -- GCP Bucket

-- Q7. It is best practice in Big Query to always cluster your data:
-- False


