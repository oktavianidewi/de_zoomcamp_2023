download-labs-data-yellow:
	chmod +x scripts/download_data.sh \
		&& ./scripts/download_data.sh "yellow" 2020 \
		&& ./scripts/download_data.sh "yellow" 2021

download-labs-data-green:
	chmod +x scripts/download_data.sh \
		&& ./scripts/download_data.sh "green" 2020 \
		&& ./scripts/download_data.sh "green" 2021

download-hw-data:
	mkdir -p data/raw/fhvhv/2021/06
	wget "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz" -O "data/raw/fhvhv/2021/06/fhvhv_tripdata_2021-06.csv.gz"

docker-spin-up:
	chmod +x scripts/build.sh && ./scripts/build.sh && docker compose up

docker-spin-down:
	docker compose down

gcloud-init: 
	gcloud auth login --no-launch-browser
	gcloud config set project dtc-de-zoomcamp-2023-376219

tf-init:
	terraform -chdir=./tf init

infra-up:
	terraform -chdir=./tf apply

upload-to-gcs:
	gsutil -m cp -r data/pq/ gs://dtc_data_lake_taxi_dtc-de-zoomcamp-2023-376219

infra-down:
	terraform -chdir=./tf destroy

infra-config:
	terraform -chdir=./tf output