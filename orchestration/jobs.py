from dagster import define_asset_job, AssetSelection

github_pipeline_job = define_asset_job(
    name="github_pipeline_job",
    selection=AssetSelection.all(),
)