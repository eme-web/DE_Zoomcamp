#### Week 3 BigQuery Home Work Solutions

##### Create an external table using the fhv 2019 data.

```
-- Creating external table referring to gcs path

CREATE OR REPLACE EXTERNAL TABLE `ferrous-phoenix-376516.taxi_trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_ferrous-phoenix-376516/fhv_data/fhv_tripdata_2019-*.csv.gz']
);
```

##### Question 1:  What is the count for fhv vehicle records for year 2019?
```
-- count for fhv vehicle records for year 2019 on external table

SELECT count(*) FROM ferrous-phoenix-376516.taxi_trips_data_all.external_fhv_tripdata;
```

```
-- count for fhv vehicle records for year 2019 on BQ table

SELECT count(*) FROM ferrous-phoenix-376516.taxi_trips_data_all.fhv_table;
```

##### Question 2:  Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
##### What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
```
-- Write a query to count the distinct number of affiliated_base_number for the entire dataset on external table.
SELECT DISTINCT COUNT(Affiliated_base_number) FROM ferrous-phoenix-376516.taxi_trips_data_all.external_fhv_tripdata;
```

```
-- Write a query to count the distinct number of affiliated_base_number for the entire dataset on BQ table.
SELECT DISTINCT COUNT(Affiliated_base_number) FROM `ferrous-phoenix-376516.taxi_trips_data_all.fhv_table`;
```

##### Question 3: How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
```
-- How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
SELECT COUNT(*) FROM `ferrous-phoenix-376516.taxi_trips_data_all.fhv_table` WHERE PUlocationID IS NULL AND DOlocationID IS NULL;
```

##### Question 4: What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
```
-- Creating a partitionWhat is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number? and cluster table
CREATE OR REPLACE TABLE ferrous-phoenix-376516.taxi_trips_data_all.fhvdata_partitoned_clustered
PARTITION BY DATE(pickup_datetime) 
CLUSTER BY Affiliated_base_number AS
SELECT * FROM ferrous-phoenix-376516.taxi_trips_data_all.external_fhv_tripdata;
```

##### Question 5: Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 03/01/2019 and 03/31/2019 (inclusive)
```
-- Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 03/01/2019 and 03/31/2019 on partitioned table
SELECT DISTINCT(Affiliated_base_number) 
FROM ferrous-phoenix-376516.taxi_trips_data_all.fhvdata_partitoned_clustered
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

```

```
-- Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 03/01/2019 and 03/31/2019 on BQ table.
SELECT DISTINCT(Affiliated_base_number) 
FROM ferrous-phoenix-376516.taxi_trips_data_all.fhv_table
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
```
