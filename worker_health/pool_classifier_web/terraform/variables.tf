variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-west1"
}

variable "domain" {
  description = "Public domain for the dashboard (e.g. pool-classifier.relops.mozilla.com)"
  type        = string
}

variable "db_tier" {
  description = "Cloud SQL machine tier"
  type        = string
  default     = "db-g1-small"
}

variable "db_password" {
  description = "Password for the pool-classifier Postgres user (stored in Secret Manager)"
  type        = string
  sensitive   = true
}

# IAP uses a Google-managed OAuth client (see lb.tf `iap { enabled = true }`),
# so no manual OAuth2 client id/secret is needed. The legacy IAP OAuth Admin
# APIs for minting custom clients were shut down in Mar 2026.

variable "iap_authorized_members" {
  description = "IAM members allowed through IAP (e.g. [\"domain:mozilla.com\"])"
  type        = list(string)
  default     = ["domain:mozilla.com"]
}

variable "cloud_run_min_instances" {
  description = "Minimum Cloud Run instances. 0 is fine — Scheduler wakes the service."
  type        = number
  default     = 0
}

variable "cloud_run_max_instances" {
  description = "Maximum Cloud Run instances"
  type        = number
  default     = 4
}

variable "cloud_run_image" {
  description = "Full Artifact Registry image reference for initial deploy. Empty falls back to the hello image."
  type        = string
  default     = ""
}

# Pool registry — mirrors worker_health/pool_classifier_web/pools.yaml. Each enabled
# pool gets a Cloud Scheduler job that POSTs to /classify/<provisioner>/<worker_type>.
variable "pools" {
  description = "Pools to schedule classify cycles for"
  type = list(object({
    id          = string
    provisioner = string
    worker_type = string
    schedule    = string
  }))
}

variable "scheduler_attempt_deadline" {
  description = "Cloud Scheduler attempt_deadline (must fit within Cloud Run request timeout)"
  type        = string
  default     = "1800s"
}
