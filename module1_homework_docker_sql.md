# Module 1 Homework

## Docker & SQL

In this homework we'll prepare the environment and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command:

```docker build --help```

Do the same for "docker run".

Which tag has the following text? - *Automatically remove the container when it exits* 

- `--delete`
- `--rc`
- `--rmc`
- `--rm`

### Notes:

```bash
docker run --help | grep "Automatically"
      --rm                             Automatically remove the container
```

:white_check_mark: `--rm`


## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use ```pip list``` ). 

What is version of the package *wheel* ?

- 0.42.0
- 1.0.0
- 23.0.1
- 58.1.0

### Notes:

```bash
$ docker run -it --entrypoint "bash" python:3.9
root@imageId:/# python --version
Python 3.9.18
root@imageId:/# pip list
Package    Version
---------- -------
pip        23.0.1
setuptools 58.1.0
wheel      0.42.0
```

:white_check_mark: 0.42.0


## Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from September 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)


## Question 3. Count records 

How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 15767
- 15612
- 15859
- 89009

### Notes:

```sql
-- count is 15612
SELECT count(*)
FROM green_taxi_trips
WHERE cast(lpep_pickup_datetime AS DATE) = '2019-09-18'
  AND cast(lpep_dropoff_datetime AS DATE) = '2019-09-18';
```

:white_check_mark: 15612


## Question 4. Largest trip for each day

Which was the pick up day with the largest trip distance
Use the pick up time for your calculations.

- 2019-09-18
- 2019-09-16
- 2019-09-26
- 2019-09-21

### Notes:

```sql
-- 2019-09-26 58,760
SELECT cast(lpep_pickup_datetime AS DATE) AS lpep_pickup_date,
       ceil(sum(trip_distance))
FROM green_taxi_trips
GROUP BY 1
ORDER BY 2 DESC;
```

:white_check_mark: 2019-09-26


## Question 5. Three biggest pick up Boroughs

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
 
- "Brooklyn" "Manhattan" "Queens"
- "Bronx" "Brooklyn" "Manhattan"
- "Bronx" "Manhattan" "Queens" 
- "Brooklyn" "Queens" "Staten Island"

### Notes:

```sql
-- These are the column fields of zones table
-- for quick reference:
-- index: bigint, LocationID: bigint,
-- Borough: text, Zone: text,
-- service_zone: text
```

```sql
-- "Brooklyn"   96,334
-- "Manhattan"  92,272
-- "Queens"     78,672
WITH filtered_green_taxi_trips AS
  (SELECT *
   FROM green_taxi_trips
   WHERE cast(lpep_pickup_datetime AS DATE) = '2019-09-18')
SELECT "Borough",
       ceil(sum(total_amount))
FROM filtered_green_taxi_trips AS g
JOIN zones AS z ON g."PULocationID" = z."LocationID"
GROUP BY 1
ORDER BY 2 DESC;
```

:white_check_mark: "Brooklyn" "Manhattan" "Queens"


## Question 6. Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Central Park
- Jamaica
- JFK Airport
- Long Island City/Queens Plaza

### Notes:

```sql
-- "JFK Airport"  62.31
 WITH pickup_during_sept_2019 AS
  (SELECT *
   FROM green_taxi_trips
   WHERE date_trunc('month', lpep_pickup_datetime) = '2019-09-01 00:00:00'),
      pickup_zone_astoria AS
  (SELECT *
   FROM pickup_during_sept_2019 AS s
   JOIN zones AS z ON s."PULocationID" = z."LocationID"
   AND z."Zone" = 'Astoria')
SELECT z."Zone",
       max(tip_amount)
FROM pickup_zone_astoria AS a
JOIN zones AS z ON a."DOLocationID" = z."LocationID"
GROUP BY 1
ORDER BY 2 DESC;
```

:white_check_mark: JFK Airport
