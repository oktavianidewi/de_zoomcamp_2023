#!/bin/bash

year=$1
# TODO: add error if year is not passed as argument

# download data from github to local
for i in {1..12}
do
   month=$(printf "%02d" $i)
   echo "Download data for $month/$year"
   wget -nv -P ./data https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_$year-$month.csv.gz
   # copy data from local to gcs
   echo "Upload data for $month/$year"
   gsutil -o GSUtil:parallel_composite_upload_threshold=300M cp data/fhv_tripdata_$year-$month.csv.gz gs://fhv_ny_taxi_dtc-de-zoomcamp-2023-376219
done
