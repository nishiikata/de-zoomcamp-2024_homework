-- Lowercasing yellow tripdata columns
ALTER TABLE trips_data_all.yellow_tripdata
    RENAME COLUMN "VendorID" TO "vendorid";
ALTER TABLE trips_data_all.yellow_tripdata
    RENAME COLUMN "RatecodeID" TO "ratecodeid";
ALTER TABLE trips_data_all.yellow_tripdata
    RENAME COLUMN "PULocationID" TO "pulocationid";
ALTER TABLE trips_data_all.yellow_tripdata
    RENAME COLUMN "DOLocationID" TO "dolocationid";
	
-- Lowercasing green tripdata columns
ALTER TABLE trips_data_all.green_tripdata
    RENAME COLUMN "VendorID" TO "vendorid";
ALTER TABLE trips_data_all.green_tripdata
    RENAME COLUMN "RatecodeID" TO "ratecodeid";
ALTER TABLE trips_data_all.green_tripdata
    RENAME COLUMN "PULocationID" TO "pulocationid";
ALTER TABLE trips_data_all.green_tripdata
    RENAME COLUMN "DOLocationID" TO "dolocationid";
	
-- Lowercasing fhv tripdata columns
ALTER TABLE trips_data_all.fhv_tripdata
    RENAME COLUMN "dropOff_datetime" TO "dropoff_datetime";
ALTER TABLE trips_data_all.fhv_tripdata
    RENAME COLUMN "PUlocationID" TO "pulocationid";
ALTER TABLE trips_data_all.fhv_tripdata
    RENAME COLUMN "DOlocationID" TO "dolocationid";
ALTER TABLE trips_data_all.fhv_tripdata
    RENAME COLUMN "SR_Flag" TO "sr_flag";
ALTER TABLE trips_data_all.fhv_tripdata
    RENAME COLUMN "Affiliated_base_number" TO "affiliated_base_number";

-- Reverting changes to yellow tripdata columns
ALTER TABLE trips_data_all.yellow_tripdata
    RENAME COLUMN "vendorid" TO "VendorID";
ALTER TABLE trips_data_all.yellow_tripdata
    RENAME COLUMN "ratecodeid" TO "RatecodeID";
ALTER TABLE trips_data_all.yellow_tripdata
    RENAME COLUMN "pulocationid" TO "PULocationID";
ALTER TABLE trips_data_all.yellow_tripdata
    RENAME COLUMN "dolocationid" TO "DOLocationID";
	
-- Reverting changes to green tripdata columns
ALTER TABLE trips_data_all.green_tripdata
    RENAME COLUMN "vendorid" TO "VendorID";
ALTER TABLE trips_data_all.green_tripdata
    RENAME COLUMN "ratecodeid" TO "RatecodeID";
ALTER TABLE trips_data_all.green_tripdata
    RENAME COLUMN "pulocationid" TO "PULocationID";
ALTER TABLE trips_data_all.green_tripdata
    RENAME COLUMN "dolocationid" TO "DOLocationID";

-- Reverting changes to green tripdata columns
ALTER TABLE trips_data_all.fhv_tripdata
    RENAME COLUMN "dropoff_datetime" TO "dropOff_datetime";
ALTER TABLE trips_data_all.fhv_tripdata
    RENAME COLUMN "pulocationid" TO "PUlocationID";
ALTER TABLE trips_data_all.fhv_tripdata
    RENAME COLUMN "dolocationid" TO "DOlocationID";
ALTER TABLE trips_data_all.fhv_tripdata
    RENAME COLUMN "sr_flag" TO "SR_Flag";
ALTER TABLE trips_data_all.fhv_tripdata
    RENAME COLUMN "affiliated_base_number" TO "Affiliated_base_number";
