resource "google_sql_database_instance" "pc" {
  name             = "pool-classifier-db"
  database_version = "POSTGRES_16"
  region           = var.region

  settings {
    tier              = var.db_tier
    availability_type = "REGIONAL"

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
