import csv
import io
import json
import os

import dlt
from dagster import EnvVar
from dlt.destinations import filesystem
from dlt.sources.rest_api import rest_api_source
from requests import Response

s3_lake_base = EnvVar("S3_LAKE_BASE").get_value().rstrip("/")
glue_database = EnvVar("GLUE_DATABASE").get_value()
aws_access_key_id = EnvVar("AWS_ACCESS_KEY_ID").get_value()
aws_secret_access_key = EnvVar("AWS_SECRET_ACCESS_KEY").get_value()
aws_region = EnvVar("AWS_REGION").get_value()

os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key
os.environ["AWS_REGION"] = aws_region
os.environ["AWS_DEFAULT_REGION"] = aws_region
os.environ.setdefault("PYICEBERG_CATALOG__DEFAULT__TYPE", "glue")
os.environ.setdefault("PYICEBERG_CATALOG__DEFAULT__WAREHOUSE", s3_lake_base)


def csv_response_to_json(response: Response) -> None:
    """Kite instrument dumps return CSV; rest_api_source expects JSON."""
    rows = list(csv.DictReader(io.StringIO(response.text)))
    response._content = json.dumps(rows).encode("utf-8")
    response.headers["Content-Type"] = "application/json"


def kite_rest_csv_source(resource_name: str, path: str, **resource_hints):
    return rest_api_source(
        {
            "client": {
                "base_url": "https://api.kite.trade",
                "headers": {"X-Kite-Version": "3"},
                "paginator": "single_page",
            },
            "resources": [
                {
                    "name": resource_name,
                    "write_disposition": "replace",
                    "endpoint": {
                        "path": path,
                        "paginator": "single_page",
                        "data_selector": "$",
                        "response_actions": [csv_response_to_json],
                    },
                    **resource_hints,
                }
            ],
        },
        name=resource_name,
    )


kite_mf_instruments = kite_rest_csv_source(
    "mf_instruments",
    "/mf/instruments",
    table_format="iceberg",
    primary_key="tradingsymbol",
)

kite_instruments = kite_rest_csv_source(
    "instruments",
    "/instruments",
    table_format="iceberg",
    primary_key="instrument_token",
)

s3_iceberg = filesystem(
    bucket_url=s3_lake_base,
    credentials={
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
        "region_name": aws_region,
    },
)


def _pipeline(**kwargs):
    """Must return a real dlt.Pipeline (dagster-dlt type-checks). Drop stale packages after a cancelled load."""
    pipe = dlt.pipeline(**kwargs)
    run = pipe.run

    def _run(*args, **kw):
        try:
            pipe.abort_packages()
        except Exception:
            try:
                pipe.drop_pending_packages()
            except Exception:
                pass
        return run(*args, **kw)

    pipe.run = _run
    return pipe


kite_pipeline = _pipeline(
    pipeline_name="kite_mf_instruments",
    destination=s3_iceberg,
    dataset_name=glue_database,
)

kite_instruments_pipeline = _pipeline(
    pipeline_name="kite_instruments",
    destination=s3_iceberg,
    dataset_name=glue_database,
)

# Fail closed on an empty/blocked Pulse page. Weekday homepages are often 200+;
# weekends are thinner (~100–150) and must still replace.
MIN_PULSE = 50


@dlt.source(name="macro_ingest")
def _macro_ingest():
    """Resources fetch at run time. Instantiate below so dagster-dlt sees selected_resources."""

    @dlt.resource(
        name="pulse_news",
        write_disposition="replace",
        table_format="iceberg",
        primary_key="pulse_id",
        columns={
            "pulse_id": {"data_type": "text"},
            "headline": {"data_type": "text"},
            "summary": {"data_type": "text"},
            "published_at": {"data_type": "timestamp"},
            "related_headlines": {"data_type": "text"},
            "related_count": {"data_type": "bigint"},
        },
    )
    def pulse_news():
        """Zerodha Pulse cards (headline, time, related). Iceberg replace."""
        from .macro_fetch import EXPECTED_NAMES, fetch_all

        pulse, macros, _ = fetch_all()
        missing = sorted(EXPECTED_NAMES - {row["name"] for row in macros})
        if len(pulse) < MIN_PULSE or missing:
            raise RuntimeError(
                f"ingest guard failed: pulse_news={len(pulse)} (need {MIN_PULSE}), "
                f"missing layer names={missing}"
            )
        yield from pulse

    @dlt.resource(
        name="macro_indices",
        write_disposition="merge",
        table_format="iceberg",
        primary_key="name",
        columns={
            "name": {"data_type": "text"},
            "group": {"data_type": "text"},
            "value": {"data_type": "double"},
            "unit": {"data_type": "text"},
            "as_of": {"data_type": "timestamp"},
            "source": {"data_type": "text"},
        },
    )
    def macro_indices():
        """India Macro Indicators + short-horizon drivers. Iceberg merge on name."""
        from .macro_fetch import fetch_all

        _, macros, drivers = fetch_all()
        yield from macros
        yield from drivers

    return pulse_news, macro_indices


macro_ingest = _macro_ingest()


macro_pipeline = _pipeline(
    pipeline_name="macro_ingest",
    destination=s3_iceberg,
    dataset_name=glue_database,
)

MIN_ETF = 50


@dlt.source(name="nse_etf")
def _nse_etf():
    @dlt.resource(
        name="nse_etfs",
        write_disposition="replace",
        table_format="iceberg",
        primary_key="symbol",
        columns={
            "symbol": {"data_type": "text"},
            "underlying": {"data_type": "text"},
            "series": {"data_type": "text"},
            "open": {"data_type": "double"},
            "high": {"data_type": "double"},
            "low": {"data_type": "double"},
            "prev_close": {"data_type": "double"},
            "ltp": {"data_type": "double"},
            "change": {"data_type": "double"},
            "pct_change": {"data_type": "double"},
            "volume": {"data_type": "bigint"},
            "traded_value": {"data_type": "double"},
            "nav": {"data_type": "double"},
            "week_high": {"data_type": "double"},
            "week_low": {"data_type": "double"},
            "pct_change_30d": {"data_type": "double"},
            "pct_change_365d": {"data_type": "double"},
            "as_of": {"data_type": "timestamp"},
            "source": {"data_type": "text"},
        },
    )
    def nse_etfs():
        """NSE ETF market watch. Iceberg replace."""
        from .macro_fetch import fetch_etfs

        rows = fetch_etfs()
        if len(rows) < MIN_ETF:
            raise RuntimeError(f"ingest guard failed: nse_etfs={len(rows)} (need {MIN_ETF})")
        yield from rows

    return nse_etfs


nse_etf = _nse_etf()

nse_etf_pipeline = _pipeline(
    pipeline_name="nse_etf",
    destination=s3_iceberg,
    dataset_name=glue_database,
)


def _yahoo_period() -> str:
    if os.environ.get("ETF_BARS_MODE") == "backfill":
        return "1y"
    from dagster._core.execution.context.compute import current_execution_context

    ctx = current_execution_context.get()
    if ctx is not None and ctx.job_name == "etf_bars_backfill":
        return "1y"
    return "5d"


def _etf_bars_frame():
    import pandas as pd
    import yfinance as yf
    from pyiceberg.exceptions import NoSuchIcebergTableError, NoSuchTableError

    from investment_data_products.dbt_plugins.iceberg_store import _catalog

    try:
        symbols = (
            _catalog()
            .load_table(f"{glue_database}.nse_etfs")
            .scan(selected_fields=("symbol",))
            .to_pandas()["symbol"]
            .dropna()
            .unique()
            .tolist()
        )
    except (NoSuchTableError, NoSuchIcebergTableError) as exc:
        raise RuntimeError("nse_etfs missing; run nse_etf_ingest first") from exc
    if not symbols:
        raise RuntimeError("nse_etfs is empty; run nse_etf_ingest first")

    tickers = [f"{s}.NS" for s in symbols]
    period = _yahoo_period()
    parts = []
    for i in range(0, len(tickers), 10):
        batch = tickers[i : i + 10]
        raw = yf.download(
            " ".join(batch),
            interval="1h",
            period=period,
            group_by="ticker",
            threads=True,
            progress=False,
            auto_adjust=False,
        )
        if raw.empty:
            continue
        if isinstance(raw.columns, pd.MultiIndex):
            fields = {str(x).lower() for x in raw.columns.get_level_values(0)}
            level = 1 if "close" in fields else 0
            df = raw.stack(level=level).rename_axis(["bar_ts", "yahoo"]).reset_index()
        else:
            df = raw.rename_axis("bar_ts").reset_index()
            df["yahoo"] = batch[0]
        df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]
        parts.append(df)
    if not parts:
        raise RuntimeError("yfinance returned no etf bars")
    df = pd.concat(parts, ignore_index=True)
    df["symbol"] = df["yahoo"].str.removesuffix(".NS")
    df["bar_ts"] = pd.to_datetime(df["bar_ts"], utc=True)
    out = df.assign(interval="1h", source="yahoo")[
        ["symbol", "bar_ts", "open", "high", "low", "close", "volume", "interval", "source"]
    ].dropna(subset=["close"])
    out["volume"] = out["volume"].fillna(0).astype("int64")
    return out


@dlt.source(name="etf_bars")
def _etf_bars():
    @dlt.resource(
        name="etf_bars",
        write_disposition="merge",
        table_format="iceberg",
        primary_key=["symbol", "bar_ts"],
        columns={
            "symbol": {"data_type": "text"},
            "bar_ts": {"data_type": "timestamp"},
            "open": {"data_type": "double"},
            "high": {"data_type": "double"},
            "low": {"data_type": "double"},
            "close": {"data_type": "double"},
            "volume": {"data_type": "bigint"},
            "interval": {"data_type": "text"},
            "source": {"data_type": "text"},
        },
    )
    def etf_bars():
        """Yahoo 1h NSE ETF bars. Iceberg merge on symbol+bar_ts."""
        yield _etf_bars_frame()

    return etf_bars


etf_bars = _etf_bars()

etf_bars_pipeline = _pipeline(
    pipeline_name="etf_bars",
    destination=s3_iceberg,
    dataset_name=glue_database,
)


if __name__ == "__main__":
    load_info = kite_instruments_pipeline.run(kite_instruments)
    print(load_info)
    print(f"Glue catalog: {glue_database}.instruments")
    print(f"Iceberg location: {s3_lake_base}/{glue_database}/instruments")
