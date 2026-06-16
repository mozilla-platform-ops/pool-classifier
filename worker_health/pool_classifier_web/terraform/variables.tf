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

# IAP OAuth client — created MANUALLY in Console (Google Auth Platform →
# Clients → Web application), since the IAP OAuth Admin API that minted clients
# was shut down Mar 2026. A plain pre-created OAuth client is still supported.
# Set the Authorized redirect URI to
# https://iap.googleapis.com/v1/oauth/clientIds/<CLIENT_ID>:handleRedirect
# (This matches hangar's working model; the Google-managed client did not work
# for @mozilla.com access in this org.)
variable "iap_oauth2_client_id" {
  description = "OAuth2 client ID for IAP (manually created in Console)"
  type        = string
}

variable "iap_oauth2_client_secret" {
  description = "OAuth2 client secret for IAP (manually created in Console)"
  type        = string
  sensitive   = true
}

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
  description = "Maximum Cloud Run instances. Kept low because the app holds a persistent DB connection per pool per instance; more instances = more connections against db-g1-small's limit."
  type        = number
  default     = 2
}

variable "cloud_run_image" {
  description = "Full Artifact Registry image reference for initial deploy. Empty falls back to the hello image."
  type        = string
  default     = ""
}

# NOTE: there is intentionally no `pools` variable. The single classify-all
# Scheduler job (scheduler.tf) lets the app read the pool list from pools.yaml
# (registry.all_pools()), so pools.yaml is the single source of truth — nothing
# to keep in sync here.

variable "scheduler_attempt_deadline" {
  description = "Cloud Scheduler attempt_deadline (must fit within Cloud Run request timeout)"
  type        = string
  default     = "1800s"
}
