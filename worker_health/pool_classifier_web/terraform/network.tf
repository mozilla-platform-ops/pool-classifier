resource "google_compute_network" "pc" {
  name                    = "pool-classifier-vpc"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.apis]
}

resource "google_compute_subnetwork" "pc" {
  name          = "pool-classifier-subnet"
  ip_cidr_range = "10.9.0.0/24"
  region        = var.region
  network       = google_compute_network.pc.id
}

# Private services access range — used by Cloud SQL private IP
resource "google_compute_global_address" "sql_private_ip_range" {
  name          = "pool-classifier-sql-private-ip"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 20
  network       = google_compute_network.pc.id
}

resource "google_service_networking_connection" "sql_vpc_peering" {
  network                 = google_compute_network.pc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.sql_private_ip_range.name]
  depends_on              = [google_project_service.apis]
}

# Serverless VPC Access Connector — lets Cloud Run reach private IPs (Cloud SQL)
resource "google_vpc_access_connector" "pc" {
  name          = "pc-connector"
  region        = var.region
  network       = google_compute_network.pc.name
  ip_cidr_range = "10.9.1.0/28"
  min_instances = 2
  max_instances = 3
  depends_on    = [google_project_service.apis]
}
