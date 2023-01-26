## Data Engineering Zoomcamp Week 1 Home Work
#### SQL Questions.

#### How many taxi trips were totally made on January 15?

```
SELECT 
	COUNT(passenger_count) as total_trips
	FROM green_trip_data
	WHERE CAST(lpep_pickup_datetime AS Date) = '2019-01-15';
```	


#### Which was the day with the largest trip distance?
```
SELECT 
	CAST(lpep_pickup_datetime AS Date) as dis_day
	FROM green_trip_data
	WHERE trip_distance = ( SELECT MAX (trip_distance)
		FROM green_trip_data
);
```

#### In 2019-01-01 how many trips had 2 and 3 passengers?
```
SELECT 
	COUNT(passenger_count) as two
	FROM green_trip_data
	WHERE passenger_count = 2 AND CAST(lpep_pickup_datetime AS Date) = '2019-01-01';


 SELECT 
	COUNT(passenger_count) as three
	FROM green_trip_data
	WHERE passenger_count = 3 AND CAST(lpep_pickup_datetime AS Date) = '2019-01-01';
```	
	


#### For the passengers picked up in the Astoria Zone which was the drop up zone that had the largest tip?
```
WITH trip_zones AS(
	SELECT
    	lpep_pickup_datetime,
    	lpep_dropoff_datetime,
    	tip_amount,
	 	(zpu."Zone")AS "pickup_zone",
    	(zdo."Zone") AS "dropoff_zone"
	FROM
    	green_trip_data t JOIN zones zpu
        	ON t."PULocationID" = zpu."LocationID"
    	JOIN zones zdo
        	ON t."DOLocationID" = zdo."LocationID")


SELECT dropoff_zone,
	 tip_amount
FROM trip_zones 
WHERE pickup_zone = 'Astoria'
GROUP BY dropoff_zone, tip_amount 
ORDER BY tip_amount DESC;
```
