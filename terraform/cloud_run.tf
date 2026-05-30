# Permissão para o Cloud Run executar jobs
resource "google_project_iam_member" "run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Cloud Run Job
resource "google_cloud_run_v2_job" "pipeline" {
  name     = "github-analytics-pipeline"
  location = var.region

  template {
    template {
      service_account = google_service_account.pipeline_sa.email
      max_retries     = 1
      timeout         = "1800s"

      containers {
        image = "docker.io/matheusmalta2002/github-analytics-pipeline:latest"

        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "GCP_DATASET_ID"
          value = "bronze_github"
        }
        env {
          name  = "GCP_BUCKET_NAME"
          value = "${var.project_id}-airbyte-state"
        }
        env {
          name  = "GITHUB_REPOS"
          value = "dbt-labs/dbt-core,dlt-hub/dlt,tobymao/sqlmesh"
        }
        env {
          name  = "DAGSTER_HOME"
          value = "/app/.dagster_home"
        }
        env {
          name  = "GITHUB_TOKEN"
          value = var.github_token
        }
        env {
          name  = "GOOGLE_APPLICATION_CREDENTIALS"
          value = "/app/github-pipeline-key.json"
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "2Gi"
          }
        }
      }
    }
  }
}

# Cloud Scheduler
resource "google_cloud_scheduler_job" "daily_trigger" {
  name      = "github-pipeline-daily"
  schedule  = "0 3 * * *"
  time_zone = "UTC"
  region    = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.pipeline.name}:run"

    oauth_token {
      service_account_email = google_service_account.pipeline_sa.email
    }
  }
}