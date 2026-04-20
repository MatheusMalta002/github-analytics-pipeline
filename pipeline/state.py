import json
import pandas as pd
from google.cloud import storage
from pipeline.config import BUCKET_NAME, STATE_FILE, START_DATE_PADRAO

def to_iso8601(value: str) -> str:
    try:
        return pd.to_datetime(value).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return START_DATE_PADRAO


def load() -> dict:
    try:
        client = storage.Client()
        blob = client.bucket(BUCKET_NAME).blob(STATE_FILE)
        if blob.exists():
            raw = json.loads(blob.download_as_text())
            return {k: to_iso8601(v) for k, v in raw.items()}
    except Exception:
        pass
    return {}


def save(state: dict) -> None:
    client = storage.Client()
    blob = client.bucket(BUCKET_NAME).blob(STATE_FILE)
    blob.upload_from_string(json.dumps(state), content_type="application/json")