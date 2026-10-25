import os

from dagster import AssetKey, AssetSelection, define_asset_job, definitions


def lake_asset_key(table: str) -> AssetKey:
    """dlt prefixes asset keys with pipeline dataset_name (`GLUE_DATABASE`)."""
    return AssetKey([os.environ["GLUE_DATABASE"], table])


kite_ingest = define_asset_job(
    name="kite_ingest",
    selection=AssetSelection.keys(
        lake_asset_key("instruments"),
        lake_asset_key("mf_instruments"),
    ),
    description="Kite /instruments and /mf/instruments Iceberg replace.",
)

macro_ingest = define_asset_job(
    name="macro_ingest",
    selection=AssetSelection.keys(
        lake_asset_key("pulse_news"),
        lake_asset_key("macro_indices"),
    ),
    description="Pulse news cards and macro_indices Iceberg (replace / merge).",
)

nse_etf_ingest = define_asset_job(
    name="nse_etf_ingest",
    selection=AssetSelection.keys(lake_asset_key("nse_etfs")),
    description="NSE ETF market watch Iceberg replace.",
)


@definitions
def defs():
    from dagster import Definitions

    return Definitions(jobs=[kite_ingest, macro_ingest, nse_etf_ingest])
