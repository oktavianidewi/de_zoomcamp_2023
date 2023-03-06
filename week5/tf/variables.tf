locals {
  data_lake_bucket = "dtc_data_lake_taxi"
}
variable "project" {
  description = "GCP Project ID"
  default     = "dtc-de-zoomcamp-2023-376219"
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
