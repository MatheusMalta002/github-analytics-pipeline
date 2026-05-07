from dagster import AssetKey
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator
from pathlib import Path

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt_github"

# Mapeamento de pasta do dbt → grupo no Dagster
DBT_GROUP_MAP = {
    "staging": "staging",
    "silver": "silver",
    "gold": "gold",
}


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: dict) -> AssetKey:
        resource_type = dbt_resource_props.get("resource_type")
        if resource_type == "source":
            table = dbt_resource_props["name"]
            return AssetKey(["bronze_github", table])
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: dict) -> str:
        # Pega o caminho do arquivo para inferir o grupo
        path = dbt_resource_props.get("path", "")
        for folder, group in DBT_GROUP_MAP.items():
            if path.startswith(folder):
                return group
        return "default"


@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
)
def github_dbt_assets(context, dbt: DbtCliResource):
    """Executa os modelos dbt: staging → silver → gold."""
    yield from dbt.cli(["run"], context=context).stream()