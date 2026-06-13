# Service account for the Cloud Run service
resource "google_service_account" "pc_run" {
  account_id   = "pool-classifier-run"
  display_name = "Pool Classifier Cloud Run"
}

# Service account used by Cloud Scheduler to invoke /classify/* with OIDC
resource "google_service_account" "pc_scheduler" {
  account_id   = "pool-classifier-scheduler"
  display_name = "Pool Classifier Cloud Scheduler"
}

# Run SA: read all Pool Classifier secrets
resource "google_secret_manager_secret_iam_binding" "run_secret_accessor" {
  for_each  = toset(local.secret_ids)
  secret_id = google_secret_manager_secret.pc[each.value].secret_id
  role      = "roles/secretmanager.secretAccessor"
  members   = ["serviceAccount:${google_service_account.pc_run.email}"]
}

# Run SA: connect to Cloud SQL
resource "google_project_iam_member" "run_sql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.pc_run.email}"
}

# Run SA: write logs
resource "google_project_iam_member" "run_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.pc_run.email}"
}

# Run SA: pull images from Artifact Registry
resource "google_artifact_registry_repository_iam_member" "run_ar_reader" {
  location   = var.region
  repository = google_artifact_registry_repository.pc.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.pc_run.email}"
}

# Scheduler SA: invoke Cloud Run (used for OIDC bearer on /classify/*).
# The LB exposes the same endpoint at the public domain too, but we send the
# Scheduler job straight at the Cloud Run URL to skip IAP entirely.
resource "google_cloud_run_v2_service_iam_member" "scheduler_invoker" {
  location = google_cloud_run_v2_service.pc.location
  name     = google_cloud_run_v2_service.pc.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pc_scheduler.email}"
}

# Cloud Build runs as the Compute Engine default SA on projects created after
# ~Apr 2024 — the legacy {num}@cloudbuild.gserviceaccount.com SA is no longer
# provisioned. Grant this SA the build/deploy roles.
locals {
  cloudbuild_sa = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
}

# builds.builder covers source-bucket read + log writing + base build perms.
resource "google_project_iam_member" "cloudbuild_builder" {
  project = var.project_id
  role    = "roles/cloudbuild.builds.builder"
  member  = local.cloudbuild_sa
}

resource "google_project_iam_member" "cloudbuild_run_admin" {
  project = var.project_id
  role    = "roles/run.admin"
  member  = local.cloudbuild_sa
}

resource "google_project_iam_member" "cloudbuild_sa_user" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = local.cloudbuild_sa
}

resource "google_project_iam_member" "cloudbuild_ar_writer" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = local.cloudbuild_sa
}

# IAP access — only @mozilla.com Google accounts
resource "google_iap_web_backend_service_iam_binding" "pc_users" {
  web_backend_service = google_compute_backend_service.pc.name
  role                = "roles/iap.httpsResourceAccessor"
  members             = var.iap_authorized_members
}
