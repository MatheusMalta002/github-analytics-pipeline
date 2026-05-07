from dagster import Definitions
from dagster_dbt import DbtCliResource

from orchestration.assets.ingestion_asset import github_raw_data
from orchestration.assets.dbt_assets import github_dbt_assets, DBT_PROJECT_DIR
from orchestration.jobs import github_pipeline_job
from orchestration.schedules import daily_schedule

defs = Definitions(
    assets=[github_raw_data, github_dbt_assets],
    jobs=[github_pipeline_job],
    schedules=[daily_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
    },
)