locals {
  image = (
    var.cloud_run_image != ""
    ? var.cloud_run_image
    : "us-docker.pkg.dev/cloudrun/container/hello"
  )
}

resource "google_cloud_run_v2_service" "pc" {
  name     = "pool-classifier"
  location = var.region

  # Only reachable via the load balancer. Scheduler also reaches the
  # service through the LB (via a non-IAP /classify/* backend).
  ingress = "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"

  template {
    service_account = google_service_account.pc_run.email
    timeout         = "1800s"

    scaling {
      min_instance_count = var.cloud_run_min_instances
      max_instance_count = var.cloud_run_max_instances
    }

    vpc_access {
      connector = google_vpc_access_connector.pc.id
      egress    = "PRIVATE_RANGES_ONLY"
    }

    containers {
      image = local.image

      resources {
        limits = {
          cpu    = "1"
          memory = "768Mi"
        }
        # Idle CPU is fine — work is driven by Cloud Scheduler, no in-process scheduler.
        cpu_idle = true
      }

      env {
        name = "DATABASE_URL"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.pc["pc-db-url"].secret_id
            version = "latest"
          }
        }
      }

      env {
        name = "TC_TOKEN_JSON"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.pc["pc-tc-token"].secret_id
            version = "latest"
          }
        }
      }

      env {
        name  = "TC_ROOT_URL"
        value = "https://firefox-ci-tc.services.mozilla.com"
      }

      env {
        name  = "POOLS_FILE"
        value = "/app/worker_health/pool_classifier_web/pools.yaml"
      }

      env {
        name  = "LOG_JSON"
        value = "true"
      }

      # OIDC validation for /classify/* — Cloud Scheduler signs each request
      # with a JWT (aud=audience, email=scheduler SA). Unset audience disables
      # validation, so always set it in production.
      env {
        name  = "CLASSIFY_OIDC_AUDIENCE"
        value = "https://${var.domain}/"
      }
      env {
        name  = "CLASSIFY_OIDC_SA_EMAIL"
        value = google_service_account.pc_scheduler.email
      }
    }
  }

  # The image is owned by the Cloud Build pipeline (cloudbuild.yaml does
  # `gcloud run deploy --image=...`), not terraform. var.cloud_run_image only
  # seeds the very first revision. Ignore image (and the client metadata that
  # `gcloud run deploy` stamps) so `terraform apply` doesn't revert deploys.
  lifecycle {
    ignore_changes = [
      template[0].containers[0].image,
      client,
      client_version,
    ]
  }

  depends_on = [
    google_secret_manager_secret_version.db_url,
    google_vpc_access_connector.pc,
    google_project_service.apis,
  ]
}
