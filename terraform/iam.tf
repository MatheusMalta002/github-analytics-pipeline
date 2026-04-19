# 1. Cria o Robô (Service Account)
resource "google_service_account" "pipeline_sa" {
  account_id   = "github-pipeline-sa"
  display_name = "Robo do Pipeline Github"
}

# 2. Dá permissão para o robô escrever no BigQuery
resource "google_project_iam_member" "bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# 3. Dá permissão para o robô rodar processos no BigQuery
resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# 4. Gera a "chave" (o arquivo .json) para usar no Python
resource "google_service_account_key" "pipeline_sa_key" {
  service_account_id = google_service_account.pipeline_sa.name
}

# 5. Mostra a chave no final
output "service_account_key" {
  value     = google_service_account_key.pipeline_sa_key.private_key
  sensitive = true
}

# 6. Dá permissão de Admin de Objetos no Bucket para o Robô
resource "google_storage_bucket_iam_member" "sa_storage_admin" {
  bucket = google_storage_bucket.airbyte_state_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_sa.email}"
}