# Secret Manager secrets for Pool Classifier.
# Terraform creates the secret containers and populates pc-db-url automatically.
# Populate pc-tc-token with:
#
#   gcloud secrets versions add pc-tc-token --data-file=$HOME/.tc_token

locals {
  secret_ids = [
    "pc-db-url",
    "pc-tc-token",
  ]
}

resource "google_secret_manager_secret" "pc" {
  for_each  = toset(local.secret_ids)
  secret_id = each.value

  replication {
    auto {}
  }

  depends_on = [google_project_service.apis]
}

# Populate db-url from the Cloud SQL instance (convenience — avoids chicken-and-egg)
resource "google_secret_manager_secret_version" "db_url" {
  secret      = google_secret_manager_secret.pc["pc-db-url"].id
  secret_data = "postgresql://pc:${var.db_password}@${google_sql_database_instance.pc.private_ip_address}/pool_classifier?sslmode=require" # pragma: allowlist secret
}
