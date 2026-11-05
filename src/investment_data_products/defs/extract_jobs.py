import os

from dagster import AssetKey, AssetSelection, Definitions, define_asset_job, definitions


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

nse_etf_ingest = define_asset_job(
    name="nse_etf_ingest",
    selection=AssetSelection.keys(lake_asset_key("nse_etfs")),
    description="NSE ETF market watch Iceberg replace.",
)

equity_price_volume_historical_ingest = define_asset_job(
    name="equity_price_volume_historical_ingest",
    selection=AssetSelection.keys(lake_asset_key("equity_price_volume")),
    description="Kite 1-minute EQ OHLCV (60d Iceberg append).",
)


@definitions
def defs():
    return Definitions(
        jobs=[
            kite_ingest,
            nse_etf_ingest,
            equity_price_volume_historical_ingest,
        ],
    )
