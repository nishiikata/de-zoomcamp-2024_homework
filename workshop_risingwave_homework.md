# RisingWave Workshop Homework

## Setting up

In order to get a static set of results, we will use historical data from the dataset.

Run the following commands:
```bash
# Load the cluster op commands.
source commands.sh
# First, reset the cluster:
clean-cluster
# Start a new cluster
start-cluster
# wait for cluster to start
sleep 5
# Seed historical data instead of real-time data
seed-kafka
# Recreate trip data table
psql -f risingwave-sql/table/trip_data.sql
# Wait for a while for the trip_data table to be populated.
sleep 5
# Check that you have 100K records in the trip_data table
# You may rerun it if the count is not 100K
psql -c "SELECT COUNT(*) FROM trip_data"
```


## Question 0

_This question is just a warm-up to introduce dynamic filter, please attempt it before viewing its solution._

What are the dropoff taxi zones at the latest dropoff times?

For this part, we will use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/).

<details>
<summary>Solution</summary>

```sql
CREATE MATERIALIZED VIEW latest_dropoff_time AS
    WITH t AS (
        SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone, latest_dropoff_time
    FROM t,
            trip_data
    JOIN taxi_zone
        ON trip_data.DOLocationID = taxi_zone.location_id
    WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;

--    taxi_zone    | latest_dropoff_time
-- ----------------+---------------------
--  Midtown Center | 2022-01-03 17:24:54
-- (1 row)
```

</details>


## Question 1

Create a materialized view to compute the average, min and max trip time **between each taxi zone**.

From this MV, find the pair of taxi zones with the highest average trip time.
You may need to use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/) for this.

Bonus (no marks): Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute,
but the max trip time is 10 minutes and 20 minutes respectively.

Options:
1. Yorkville East, Steinway
2. Murray Hill, Midwood
3. East Flatbush/Farragut, East Harlem North
4. Midtown Center, University Heights/Morris Heights

p.s. The trip time between taxi zones does not take symmetricity into account, i.e. `A -> B` and `B -> A` are considered different trips. This applies to subsequent questions as well.

### Notes:

```sql
CREATE MATERIALIZED VIEW trip_describe AS
SELECT
  taxi_zone_pu.Zone as pickup_zone,
  taxi_zone_do.Zone as dropoff_zone,
  count(*) as number_of_trips,
  avg(tpep_dropoff_datetime - tpep_pickup_datetime) as avg_trip_time,
  min(tpep_dropoff_datetime - tpep_pickup_datetime) as min_trip_time,
  max(tpep_dropoff_datetime - tpep_pickup_datetime) as max_trip_time
FROM trip_data
JOIN taxi_zone as taxi_zone_pu
  ON trip_data.PULocationID = taxi_zone_pu.location_id
JOIN taxi_zone as taxi_zone_do
  ON trip_data.DOLocationID = taxi_zone_do.location_id
GROUP BY pickup_zone, dropoff_zone;
```

```sql
CREATE MATERIALIZED VIEW avg_trip_time_max AS
WITH highest_average_trip_time AS (
  SELECT max(avg_trip_time) AS max
  FROM trip_describe
)
SELECT
  pickup_zone,
  dropoff_zone,
  number_of_trips,
  avg_trip_time
FROM trip_describe, highest_average_trip_time
WHERE avg_trip_time = max;
```

```bash
# Q1 Answer: Yorkville East, Steinway
# Q2 Answer: 1 
dev=> SELECT * FROM avg_trip_time_max;
  pickup_zone   | dropoff_zone | number_of_trips | avg_trip_time
----------------+--------------+-----------------+---------------
 Yorkville East | Steinway     |               1 | 23:59:33
(1 row)
```

:white_check_mark: Yorkville East, Steinway

```sql
-- Bonus question answer:
CREATE MATERIALIZED VIEW trip_anomalies AS
SELECT *
FROM trip_describe
WHERE
  min_trip_time > max_trip_time OR
  avg_trip_time NOT BETWEEN min_trip_time AND max_trip_time;
```


## Question 2

Recreate the MV(s) in question 1, to also find the **number of trips** for the pair of taxi zones with the highest average trip time.

Options:
1. 5
2. 3
3. 10
4. 1

### Notes:

:white_check_mark: 4. 1


## Question 3

From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?
For example if the latest pickup time is 2020-01-01 17:00:00,
then the query should return the top 3 busiest zones from 2020-01-01 00:00:00 to 2020-01-01 17:00:00.

HINT: You can use [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/)
to create a filter condition based on the latest pickup time.

NOTE: For this question `17 hours` was picked to ensure we have enough data to work with.

Options:
1. Clinton East, Upper East Side North, Penn Station
2. LaGuardia Airport, Lincoln Square East, JFK Airport
3. Midtown Center, Upper East Side South, Upper East Side North
4. LaGuardia Airport, Midtown Center, Upper East Side North

### Notes:

```sql
CREATE MATERIALIZED VIEW busiest_pickups AS
WITH latest_pickup_time AS (
  SELECT max(tpep_pickup_datetime) AS max
  FROM trip_data
)
SELECT
  taxi_zone.Zone as pickup_zone,
  count(*)
FROM latest_pickup_time,
     trip_data
JOIN taxi_zone
  ON trip_data.PULocationID = taxi_zone.location_id
WHERE tpep_pickup_datetime >= max - interval '17 hours'
GROUP BY pickup_zone;
```

```bash
dev=> SELECT * FROM busiest_pickups ORDER BY count DESC LIMIT 3;
     pickup_zone     | count
---------------------+-------
 LaGuardia Airport   |    19
 Lincoln Square East |    17
 JFK Airport         |    17
(3 rows)
```

:white_check_mark: 2. LaGuardia Airport, Lincoln Square East, JFK Airport
