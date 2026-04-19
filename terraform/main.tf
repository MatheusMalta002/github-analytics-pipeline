# Layer 1: Bronze (Dados brutos)
resource "google_bigquery_dataset" "bronze" {
  dataset_id                  = "bronze_github"
  friendly_name               = "Bronze - Github"
  description                 = "Dados brutos extraídos da API do Github"
  location                    = var.region
  delete_contents_on_destroy  = true
}

# Layer 2: Silver (Dados limpos/transformados)
resource "google_bigquery_dataset" "silver" {
  dataset_id                  = "silver_github"
  friendly_name               = "Silver - Github"
  description                 = "Dados padronizados e modelados via dbt"
  location                    = var.region
  delete_contents_on_destroy  = true
}

# Layer 3: Gold (Dados agregados/métricas)
resource "google_bigquery_dataset" "gold" {
  dataset_id                  = "gold_github"
  friendly_name               = "Gold - Github"
  description                 = "Métricas finais e tabelas prontas para dashboard"
  location                    = var.region
  delete_contents_on_destroy  = true
}

# Cria o Bucket para o Estado do Airbyte
resource "google_storage_bucket" "airbyte_state_bucket" {
  name          = "${var.project_id}-airbyte-state"
  location      = var.region
  force_destroy = true 

  public_access_prevention = "enforced"
}