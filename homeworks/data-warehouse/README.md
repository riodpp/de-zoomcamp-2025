# Solution

## Question 1:
Question 1: What is count of records for the 2024 Yellow Taxi Data?
- 65,623
- 840,402
- 20,332,093
- 85,431,289

## Solution:
### Query
```sql
select count(1) from `data_engineering_zoomcamp.yellow_tripdata_2024_materialized`
```
<img width="855" alt="image" src="https://github.com/user-attachments/assets/5851ca87-c4b1-41c1-a9b8-05b3f0f0dc27" />


## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 155.12 MB for the Materialized Table
- 2.14 GB for the External Table and 0MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table

## Solution
### Query
Query external table
```sql  
    select count(distinct PULocationID) from `data_engineering_zoomcamp.yellow_tripdata_2024_external`
```
<img width="1041" alt="image" src="https://github.com/user-attachments/assets/8153c7a3-10aa-4d2d-8299-4690f1c15971" />


Query materialized table
```sql
    select count(distinct PULocationID) from `data_engineering_zoomcamp.yellow_tripdata_2024_materialized`
```
<img width="1044" alt="image" src="https://github.com/user-attachments/assets/7b5af9af-2d2f-478a-aa59-92ff695c8a04" />


## Question 3:
Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
- BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires 
reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
- BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, 
doubling the estimated bytes processed.
- BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
- When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

## Solution
A, Because of the columnar database.
query ome column from materialized
<img width="1044" alt="image" src="https://github.com/user-attachments/assets/85b87ff4-445f-4443-97cf-859c1cd289d4" />

query two column
<img width="1041" alt="image" src="https://github.com/user-attachments/assets/5baee200-17c5-4b15-a2b8-acc2fdce92db" />


## Question 4:
How many records have a fare_amount of 0?
- 128,210
- 546,578
- 20,188,016
- 8,333

## Solution
### Query
```sql
    select count(1) from `data_engineering_zoomcamp.yellow_tripdata_2024_materialized` where fare_amount = 0
```
<img width="826" alt="image" src="https://github.com/user-attachments/assets/61aa2391-e69c-42c0-bd81-cb9deb86e333" />


## Question 5:
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)
- Partition by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on tpep_dropoff_datetime Partition by VendorID
- Partition by tpep_dropoff_datetime and Partition by VendorID

## Solution
SQL to create table partition
```sql
CREATE OR REPLACE TABLE `data_engineering_zoomcamp.yellow_tripdata_2024_partitioned`
(
    VendorID INT64,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT64,
    trip_distance FLOAT64,
    RatecodeID INT64,
    store_and_fwd_flag STRING,
    PULocationID INT64,
    DOLocationID INT64,
    payment_type INT64,
    fare_amount FLOAT64,
    extra FLOAT64,
    mta_tax FLOAT64,
    tip_amount FLOAT64,
    tolls_amount FLOAT64,
    improvement_surcharge FLOAT64,
    total_amount FLOAT64,
    congestion_surcharge FLOAT64,
    Airport_fee FLOAT64
)
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS (
  SELECT * FROM `data_engineering_zoomcamp.yellow_tripdata_2024_materialized`
)
```
Since queries always filter by `tpep_dropoff_datetime`, partitioning by `DATE(tpep_dropoff_datetime)` ensures that only the relevant partitions are scanned, reducing query costs. Also, Since queries order by VendorID, clustering helps store related VendorID values together, making sorting operations much faster.

## Question 6:
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime
2024-03-01 and 2024-03-15 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
- 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

## Solution
### Query
Query non-partitioned table
```sql
    select distinct VendorID from `data_engineering_zoomcamp.yellow_tripdata_2024_materialized` where tpep_dropoff_datetime between '2024-03-01' and '2024-03-15'
```
<img width="1046" alt="image" src="https://github.com/user-attachments/assets/b31d7c3f-f564-4284-a6c9-9765529dc0d7" />

Query partitioned table
```sql
    select distinct VendorID from `data_engineering_zoomcamp.yellow_tripdata_2024_partitioned` where tpep_dropoff_datetime between '2024-03-01' and '2024-03-15'
```
<img width="1047" alt="image" src="https://github.com/user-attachments/assets/5ba2393e-6f87-451d-93b3-27e5389ef820" />


## Question 7: 
Where is the data stored in the External Table you created?

- Big Query
- Container Registry
- GCP Bucket
- Big Table

## Solution
External table is stored in GCP Bucket

## Question 8:
It is best practice in Big Query to always cluster your data:
- True
- False

## Solution
False, depend on the data size and uniqueness of the value that want to be clustered. You alse need to consider how frequent the data will be filtered.

