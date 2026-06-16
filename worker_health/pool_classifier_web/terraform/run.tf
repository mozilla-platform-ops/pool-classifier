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

    # NOTE: this block is in `lifecycle.ignore_changes` below (Cloud Build's
    # `gcloud run deploy` re-stamps scaling, causing perpetual drift). It seeds
    # only the first revision — editing these values here and re-applying is a
    # NO-OP. To change scaling later, use:
    #   gcloud run services update pool-classifier --region=us-west1 \
    #     --min-instances=N --max-instances=N --project=relops-pool-classifier
    # (or temporarily remove `template[0].scaling` from ignore_changes, apply, re-add).
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
  # seeds the very first revision. Ignore image (and the client metadata +
  # scaling block that `gcloud run deploy` re-stamps) so `terraform apply`
  # doesn't churn on deploy-driven drift.
  lifecycle {
    ignore_changes = [
      template[0].containers[0].image,
      template[0].scaling,
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

# IAP in front of Cloud Run requires the IAP service agent to (a) be provisioned
# and (b) hold run.invoker on the service — IAP invokes Cloud Run on behalf of
# authenticated users. Without this you get "The IAP service account is not
# provisioned." https://cloud.google.com/iap/docs/enabling-cloud-run
resource "google_project_service_identity" "iap" {
  provider = google-beta
  service  = "iap.googleapis.com"

  depends_on = [google_project_service.apis]
}

resource "google_cloud_run_v2_service_iam_member" "iap_invoker" {
  location = google_cloud_run_v2_service.pc.location
  name     = google_cloud_run_v2_service.pc.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_project_service_identity.iap.email}"
}
