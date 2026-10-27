import os

from dagster import (
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
    definitions,
)


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

_etf_bars_sel = AssetSelection.keys(
    lake_asset_key("nse_etfs"),
    lake_asset_key("etf_bars"),
)

etf_bars_backfill = define_asset_job(
    name="etf_bars_backfill",
    selection=_etf_bars_sel,
    description="NSE ETF watch then Yahoo 1h bars (~1y, Iceberg merge).",
)

etf_bars_hourly = define_asset_job(
    name="etf_bars_hourly",
    selection=_etf_bars_sel,
    description="NSE ETF watch then Yahoo 1h bars (5d window, Iceberg merge).",
)

etf_bars_hourly_schedule = ScheduleDefinition(
    name="etf_bars_hourly_ist",
    target=_etf_bars_sel,
    cron_schedule="0 9-16 * * 1-5",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Weekdays 9:00–16:00 IST: nse_etfs then etf_bars (5d Yahoo).",
)


@definitions
def defs():
    from dagster import Definitions

    return Definitions(
        jobs=[
            kite_ingest,
            macro_ingest,
            nse_etf_ingest,
            etf_bars_backfill,
            etf_bars_hourly,
        ],
        schedules=[etf_bars_hourly_schedule],
    )
