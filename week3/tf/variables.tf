locals {
  data_lake_bucket = "fhv_ny_taxi"
}
variable "project" {
  description = "GCP Project ID"
  default     = "DTC DE Zoomcamp 2023"
  type        = string
}
variable "region" {
  description = "Region for GCP resources."
  default     = "asia-southeast1"
  type        = string
}
variable "storage_class" {
  description = "storage class for your bucket"
  default     = "STANDARD"
}
variable "bq_dataset" {
  description = "BigQuery dataset"
  type        = string
  default     = "fhv_tripdata"
}
variable "tripdata" {
  description = "BigQuery table"
  type        = string
  default     = "tripdata"
}
