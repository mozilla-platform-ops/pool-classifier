# One Cloud Scheduler job per enabled pool. Posts to /classify/<provisioner>/<worker_type>
# via the public LB; the URL map routes that path to a non-IAP backend.
resource "google_cloud_scheduler_job" "classify" {
  for_each = { for p in var.pools : p.id => p }

  name             = "pool-classifier-${each.value.id}"
  description      = "Classify cycle for ${each.value.provisioner}/${each.value.worker_type}"
  schedule         = each.value.schedule
  time_zone        = "Etc/UTC"
  attempt_deadline = var.scheduler_attempt_deadline
  region           = var.region

  retry_config {
    retry_count = 1
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.domain}/classify/${each.value.provisioner}/${each.value.worker_type}"

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
