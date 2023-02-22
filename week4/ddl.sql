CREATE TABLE `dtc-de-zoomcamp-2023-376219.trips_data_all.yellow_tripdata_bq`
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
  congestion_surcharge FLOAT64
);


CREATE TABLE `dtc-de-zoomcamp-2023-376219.trips_data_all.green_tripdata_bq`
(
  VendorID INT64,
  lpep_pickup_datetime TIMESTAMP,
  lpep_dropoff_datetime TIMESTAMP,
  store_and_fwd_flag INT64,
  RatecodeID INT64,
  PULocationID INT64,
  DOLocationID INT64,
  passenger_count INT64,
  trip_distance FLOAT64,
  fare_amount FLOAT64,
  extra FLOAT64,
  mta_tax FLOAT64,
  tip_amount FLOAT64,
  tolls_amount FLOAT64,
  ehail_fee FLOAT64,
  improvement_surcharge FLOAT64,
  total_amount FLOAT64,
  payment_type INT64,
  trip_type INT64,
  congestion_surcharge FLOAT64
);
