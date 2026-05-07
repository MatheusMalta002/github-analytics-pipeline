from dagster import ScheduleDefinition
from orchestration.jobs import github_pipeline_job

daily_schedule = ScheduleDefinition(
    job=github_pipeline_job,
    cron_schedule="0 3 * * *",  # todo dia às 03:00 UTC
)