import airbyte as ab
import pandas as pd
from google.cloud import bigquery

from pipeline import state
from pipeline.config import (
    PROJECT_ID, DATASET_ID, REPOS,
    START_DATE_PADRAO, CURSOR_FIELDS
)

bq = bigquery.Client(project=PROJECT_ID)

def _start_date(last_state: dict) -> str:
    cursors = [v for v in last_state.values() if v]
    return min(cursors) if cursors else START_DATE_PADRAO


def _upload(df: pd.DataFrame, stream_name: str) -> None:
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{stream_name}"
    job = bq.load_table_from_dataframe(
        df, table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=True,
        )
    )
    job.result()


def _cursor(df: pd.DataFrame, stream_name: str) -> str | None:
    field = CURSOR_FIELDS.get(stream_name)
    if field and field in df.columns:
        return pd.to_datetime(df[field].max()).strftime("%Y-%m-%dT%H:%M:%SZ")
    return None


def run() -> None:
    last_state = state.load()
    start_date = _start_date(last_state)

    source = ab.get_source(
        "source-github",
        config={
            "credentials": {"personal_access_token": ab.get_secret("GITHUB_TOKEN")},
            "repositories": REPOS,
            "start_date": start_date,
        },
    )
    source.select_streams(list(CURSOR_FIELDS.keys()))

    result = source.read(cache=ab.new_local_cache())

    new_state = {}
    for stream_name, stream_reader in result.streams.items():
        df = stream_reader.to_pandas()

        if df.empty:
            if stream_name in last_state:
                new_state[stream_name] = last_state[stream_name]
            continue

        _upload(df, stream_name)

        cursor = _cursor(df, stream_name)
        new_state[stream_name] = cursor if cursor else last_state.get(stream_name)

    if new_state:
        state.save(new_state)

if __name__ == "__main__":
    run()