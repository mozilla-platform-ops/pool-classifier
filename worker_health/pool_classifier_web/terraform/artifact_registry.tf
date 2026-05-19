resource "google_artifact_registry_repository" "pc" {
  location      = var.region
  repository_id = "pool-classifier"
  format        = "DOCKER"
  description   = "Pool Classifier container images"

  cleanup_policies {
    id     = "keep-last-10"
    action = "KEEP"
    most_recent_versions {
      keep_count = 10
    }
  }

  depends_on = [google_project_service.apis]
}
