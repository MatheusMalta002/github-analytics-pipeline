from dagster import define_asset_job, AssetSelection, in_process_executor

github_pipeline_job = define_asset_job(
    name="github_pipeline_job",
    selection=AssetSelection.all(),
    executor_def=in_process_executor,
)