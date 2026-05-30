variable "project_id" {
  description = "O ID do projeto no GCP"
  type        = string
  default     = "pipeline-github-493713"
}

variable "region" {
  description = "Região padrão dos recursos"
  type        = string
  default     = "us-central1"
}

variable "github_token" {
  description = "Token de acesso da API do GitHub"
  type        = string
  sensitive   = true
}