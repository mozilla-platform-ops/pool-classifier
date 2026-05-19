output "load_balancer_ip" {
  description = "Point your DNS A record here: <domain> → this IP"
  value       = google_compute_global_address.pc.address
}

output "artifact_registry_hostname" {
  description = "Docker registry hostname for image pushes"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/pool-classifier"
}

output "cloud_run_url" {
  description = "Direct Cloud Run URL (used by Cloud Scheduler for OIDC-authenticated POSTs)"
  value       = google_cloud_run_v2_service.pc.uri
}

output "db_private_ip" {
  description = "Cloud SQL private IP (reachable only from the VPC)"
  value       = google_sql_database_instance.pc.private_ip_address
  sensitive   = true
}

output "populate_secrets_commands" {
  description = "Run these after first apply to populate secret values"
  value       = <<-EOT
    # Taskcluster token (JSON: {"clientId":"...","accessToken":"..."}):
    gcloud secrets versions add pc-tc-token --data-file=$HOME/.tc_token
  EOT
}
