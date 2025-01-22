variable "location" {
  default     = "US"
  description = "Project location"
}

variable "region" {
  description = "Project region"
  default = "us-central1"
}

variable "project" {
  description = "Project ID"
  default = "terraform-demo-448518"
}

variable "bq_dataset_name" {
  description = "BiqQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "Google Cloud Storage bucket name"
  default     = "terraform-demo-448518-bucket"
}

variable "gcs_storage_class" {
  description = "GCS storage class type"
  default     = "STANDARD"
}

variable "credentials" {
  description = "My creds"
  default = "./keys/creds.json"
}