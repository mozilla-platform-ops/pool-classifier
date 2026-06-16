# Global static IP for the load balancer
resource "google_compute_global_address" "pc" {
  name = "pool-classifier-ip"
}

# Managed SSL certificate
resource "google_compute_managed_ssl_certificate" "pc" {
  name = "pool-classifier-cert"
  managed {
    domains = [var.domain]
  }
}

# Serverless NEG — maps LB backends to the Cloud Run service
resource "google_compute_region_network_endpoint_group" "pc" {
  name                  = "pool-classifier-neg"
  network_endpoint_type = "SERVERLESS"
  region                = var.region

  cloud_run {
    service = google_cloud_run_v2_service.pc.name
  }
}

# Default backend — IAP-protected (browsers, dashboard pages)
resource "google_compute_backend_service" "pc" {
  name                  = "pool-classifier-backend"
  protocol              = "HTTPS"
  load_balancing_scheme = "EXTERNAL_MANAGED"
  security_policy       = google_compute_security_policy.pc.id

  backend {
    group = google_compute_region_network_endpoint_group.pc.id
  }

  # Manually-created OAuth client (Console → Google Auth Platform → Clients).
  # The Google-managed client (`enabled = true` only) authenticated @mozilla.com
  # users but authorization still failed in this org; hangar uses an explicit
  # client and works, so we match that. The IAP OAuth Admin API is dead, but a
  # pre-created plain OAuth client is still supported here.
  iap {
    enabled              = true
    oauth2_client_id     = var.iap_oauth2_client_id
    oauth2_client_secret = var.iap_oauth2_client_secret
  }

  log_config {
    enable      = true
    sample_rate = 1.0
  }
}

# Scheduler backend — same NEG, no IAP. Cloud Scheduler hits this path with
# an OIDC bearer token; the app is responsible for validating the token.
resource "google_compute_backend_service" "pc_classify" {
  name                  = "pool-classifier-classify-backend"
  protocol              = "HTTPS"
  load_balancing_scheme = "EXTERNAL_MANAGED"
  security_policy       = google_compute_security_policy.pc.id

  backend {
    group = google_compute_region_network_endpoint_group.pc.id
  }

  log_config {
    enable      = true
    sample_rate = 1.0
  }
}

# URL map: route /classify/* to the no-IAP backend, everything else through IAP.
resource "google_compute_url_map" "pc" {
  name            = "pool-classifier-url-map"
  default_service = google_compute_backend_service.pc.id

  host_rule {
    hosts        = [var.domain]
    path_matcher = "main"
  }

  path_matcher {
    name            = "main"
    default_service = google_compute_backend_service.pc.id

    path_rule {
      paths   = ["/classify/*"]
      service = google_compute_backend_service.pc_classify.id
    }
  }
}

resource "google_compute_target_https_proxy" "pc" {
  name             = "pool-classifier-https-proxy"
  url_map          = google_compute_url_map.pc.id
  ssl_certificates = [google_compute_managed_ssl_certificate.pc.id]
}

resource "google_compute_global_forwarding_rule" "pc_https" {
  name                  = "pool-classifier-https"
  target                = google_compute_target_https_proxy.pc.id
  port_range            = "443"
  ip_address            = google_compute_global_address.pc.id
  load_balancing_scheme = "EXTERNAL_MANAGED"
}

# HTTP → HTTPS redirect
resource "google_compute_url_map" "pc_redirect" {
  name = "pool-classifier-http-redirect"

  default_url_redirect {
    https_redirect         = true
    redirect_response_code = "MOVED_PERMANENTLY_DEFAULT"
    strip_query            = false
  }
}

resource "google_compute_target_http_proxy" "pc_redirect" {
  name    = "pool-classifier-http-proxy"
  url_map = google_compute_url_map.pc_redirect.id
}

resource "google_compute_global_forwarding_rule" "pc_http" {
  name                  = "pool-classifier-http"
  target                = google_compute_target_http_proxy.pc_redirect.id
  port_range            = "80"
  ip_address            = google_compute_global_address.pc.id
  load_balancing_scheme = "EXTERNAL_MANAGED"
}
