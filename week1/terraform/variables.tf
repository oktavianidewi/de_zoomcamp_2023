locals {
  data_lake_bucket = "dtc_data_lake"
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
variable "BQ_DATASET" {
  description = "BigQuery dataset that raw data will be written to"
  type        = string
  default     = "trips_data_all"
}
