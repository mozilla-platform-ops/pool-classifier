# A single Cloud Scheduler job drives a sequential classify of ALL enabled pools
# via POST /classify-all. The app reads the pool list from pools.yaml
# (registry.all_pools()) and walks them one at a time.
#
# This replaces the previous 38-job-per-pool fan-out, which fired concurrently
# and (a) exhausted the Postgres connection budget and (b) hammered the
# Taskcluster API. Sequential single-job processing mirrors pc_fetch_data.sh.
# Because pools now come from pools.yaml, there is no `var.pools` to keep in sync.
resource "google_cloud_scheduler_job" "classify_all" {
  name             = "pool-classifier-classify-all"
  description      = "Sequential classify cycle over all enabled pools"
  schedule         = "*/15 * * * *"
  time_zone        = "Etc/UTC"
  attempt_deadline = var.scheduler_attempt_deadline
  region           = var.region

  retry_config {
    retry_count = 1
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.domain}/classify-all"

    oidc_token {
      service_account_email = google_service_account.pc_scheduler.email
      audience              = "https://${var.domain}/"
    }
  }

  depends_on = [
    google_project_service.apis,
    google_compute_global_forwarding_rule.pc_https,
  ]
}
