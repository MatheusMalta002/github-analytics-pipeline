import os
from dotenv import load_dotenv

load_dotenv(override=True)

PROJECT_ID        = os.environ["GCP_PROJECT_ID"]
DATASET_ID        = os.environ["GCP_DATASET_ID"]
BUCKET_NAME       = os.environ["GCP_BUCKET_NAME"]
REPOS             = os.environ["GITHUB_REPOS"].split(",")
START_DATE_PADRAO = os.environ.get("START_DATE", "2024-01-01T00:00:00Z")
STATE_FILE        = "github_state.json"

CURSOR_FIELDS = {
    "commits":         "created_at",
    "issues":          "updated_at",
    "comments":        "updated_at",
    "review_comments": "updated_at",
}