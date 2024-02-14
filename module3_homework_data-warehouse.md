# Module 3 Homework

## Week 3 Homework
ATTENTION: At the end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. This repository should contain your code for solving the homework. If your solution includes code that is not in file format (such as SQL queries or shell commands), please include these directly in the README file of your repository.

<b><u>Important Note:</b></u> <p> For this homework we will be using the 2022 Green Taxi Trip Record Parquet Files from the New York
City Taxi Data found here: </br> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page </br>
If you are using orchestration such as Mage, Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You will need to use the PARQUET option files when creating an External Table</br>

<b>SETUP:</b></br>
Create an external table using the Green Taxi Trip Records Data for 2022. </br>

```sql
CREATE OR REPLACE EXTERNAL TABLE `dtc-decamp2024-d-0214.ny_taxi.green_taxi_2022_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-zoomcamp-2024-sea/ny_taxi/green_tripdata_2022-[0-1][1-9].parquet']
);
```

Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table). </br>
</p>

```sql
CREATE OR REPLACE TABLE `dtc-decamp2024-d-0214.ny_taxi.green_taxi_2022_non_partitioned`
AS (SELECT *
    FROM `dtc-decamp2024-d-0214.ny_taxi.green_taxi_2022_external`);
```


## Question 1:
Question 1: What is count of records for the 2022 Green Taxi Data??
- 65,623,481
- 840,402
- 1,936,423
- 253,647

### Notes:

```sql
SELECT count(*)
FROM `dtc-decamp2024-d-0214.ny_taxi.green_taxi_2022_external`;
```

:white_check_mark: 840,402


## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 6.41MB for the Materialized Table
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table

### Notes:

```sql
-- External Table Query
SELECT count(DISTINCT PULocationID)
FROM `dtc-decamp2024-d-0214.ny_taxi.green_taxi_2022_external`;

-- Materialized Table Query
SELECT count(DISTINCT PULocationID)
FROM `dtc-decamp2024-d-0214.ny_taxi.green_taxi_2022_non_partitioned`;
```

:white_check_mark: 0 MB for the External Table and 6.41MB for the Materialized Table


## Question 3:
How many records have a fare_amount of 0?
- 12,488
- 128,219
- 112
- 1,622

### Notes:

```sql
SELECT count(*)
FROM `dtc-decamp2024-d-0214.ny_taxi.green_taxi_2022_external`
WHERE fare_amount = 0;
```

:white_check_mark: 1,622


## Question 4:
What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
- Cluster on lpep_pickup_datetime Partition by PUlocationID
- Partition by lpep_pickup_datetime  Cluster on PUlocationID
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

### Notes:

```sql
CREATE OR REPLACE TABLE `dtc-decamp2024-d-0214.ny_taxi.green_taxi_2022_partitioned` 
PARTITION BY cast(lpep_pickup_datetime AS DATE)
CLUSTER BY PULocationID AS (
  SELECT *
  FROM dtc-decamp2024-d-0214.ny_taxi.green_taxi_2022_non_partitioned
);
```

:white_check_mark: Partition by lpep_pickup_datetime  Cluster on PUlocationID


## Question 5:
Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime
06/01/2022 and 06/30/2022 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table

### Notes:

```sql
-- Materialized Table Query
SELECT DISTINCT PULocationID
FROM dtc-decamp2024-d-0214.ny_taxi.green_taxi_2022_non_partitioned
WHERE cast(lpep_pickup_datetime AS DATE) BETWEEN '2022-06-01' AND '2022-06-30';

-- Partitioned Table Query
SELECT DISTINCT PULocationID
FROM dtc-decamp2024-d-0214.ny_taxi.green_taxi_2022_partitioned
WHERE cast(lpep_pickup_datetime AS DATE) BETWEEN '2022-06-01' AND '2022-06-30';
```

:white_check_mark: 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table


## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- Big Table
- Container Registry

### Notes:

```sql
-- URI uses GCP Bucket path
CREATE EXTERNAL TABLE `PROJECT_ID.DATASET.EXTERNAL_TABLE_NAME`
  OPTIONS (
    format ="TABLE_FORMAT",
    uris = ['BUCKET_PATH'[,...]]
    );
```

:white_check_mark: GCP Bucket


## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- False

### Notes:

> "Table with data size < 1 GB, don’t show significant improvement with partitioning and clustering"

:white_check_mark: False


## (Bonus: Not worth points) Question 8:
No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

### Notes:

It will estimate 0 bytes since the table is already cached.
