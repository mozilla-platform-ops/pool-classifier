resource "google_sql_database_instance" "pc" {
  name             = "pool-classifier-db"
  database_version = "POSTGRES_16"
  region           = var.region

  settings {
    # ENTERPRISE (not ENTERPRISE_PLUS) is required for the shared-core db-g1-small
    # tier. ZONAL because shared-core tiers don't support HA; this is a self-healing
    # monitoring dashboard (data re-derived every 15 min) with 7-day backups + PITR.
    edition           = "ENTERPRISE"
    tier              = var.db_tier
    availability_type = "ZONAL"

    # db-g1-small's default max_connections is very low (~25-50). The app holds
    # a persistent connection per pool per gunicorn worker (cached classifiers),
    # so it needs more headroom. Paired with workers=1 + max_instances=2 +
    # staggered schedules to keep demand under this ceiling. (Changing this
    # restarts the instance.)
    database_flags {
      name  = "max_connections"
      value = "100"
    }

    ip_configuration {
      ipv4_enabled                                  = false
      private_network                               = google_compute_network.pc.id
      enable_private_path_for_google_cloud_services = true
      ssl_mode                                      = "ENCRYPTED_ONLY"
    }

    backup_configuration {
      enabled                        = true
      start_time                     = "02:00"
      point_in_time_recovery_enabled = true
      backup_retention_settings {
        retained_backups = 7
      }
    }

    maintenance_window {
      day          = 7 # Sunday
      hour         = 3
      update_track = "stable"
    }

    insights_config {
      query_insights_enabled = true
    }

    deletion_protection_enabled = true
  }

  depends_on = [google_service_networking_connection.sql_vpc_peering]
}

resource "google_sql_database" "pool_classifier" {
  name     = "pool_classifier"
  instance = google_sql_database_instance.pc.name
}

resource "google_sql_user" "pc" {
  name     = "pc"
  instance = google_sql_database_instance.pc.name
  password = var.db_password
}
