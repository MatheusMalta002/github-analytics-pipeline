from dagster import asset, AssetOut, multi_asset


@multi_asset(
    outs={
        "commits":         AssetOut(group_name="bronze", key_prefix="bronze_github"),
        "issues":          AssetOut(group_name="bronze", key_prefix="bronze_github"),
        "comments":        AssetOut(group_name="bronze", key_prefix="bronze_github"),
        "review_comments": AssetOut(group_name="bronze", key_prefix="bronze_github"),
    },
    compute_kind="python",
)
def github_raw_data(context):
    """Extrai dados da API do GitHub e carrega nas tabelas bronze do BigQuery."""
    from pipeline.ingestion import run
    context.log.info("Iniciando ingestão do GitHub...")
    run()
    context.log.info("Ingestão finalizada.")
    return None, None, None, None