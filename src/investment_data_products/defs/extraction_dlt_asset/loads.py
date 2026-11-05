import asyncio
import csv
import io
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import dlt
import httpx
from dagster import EnvVar
from dlt.destinations import filesystem
from dlt.sources.rest_api import rest_api_source
from requests import Response
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import ReadTimeout

_log = logging.getLogger(__name__)

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

_DLT_PIPELINES = Path(__file__).resolve().parents[4] / ".dlt" / "pipelines"


def _bind_drop(pipe):
    """Windows can lock dlt gzip files; skip wipe rather than fail the asset."""
    orig_drop = pipe.drop

    def drop(*args, **kwargs):
        last = None
        for delay in (0, 0.5, 1.5, 3.0):
            if delay:
                time.sleep(delay)
            try:
                return orig_drop(*args, **kwargs)
            except PermissionError as exc:
                last = exc
        try:
            pipe.abort_packages()
        except Exception:
            pass
        try:
            pipe.drop_pending_packages()
        except Exception:
            pass
        _log.warning("dlt drop skipped after file lock: %s", last)
        return pipe

    pipe.drop = drop
    return pipe


def _pipeline(**kwargs):
    """Must return a real dlt.Pipeline (dagster-dlt type-checks). Drop stale packages after a cancelled load."""
    _DLT_PIPELINES.mkdir(parents=True, exist_ok=True)
    kwargs.setdefault("pipelines_dir", str(_DLT_PIPELINES))
    pipe = _bind_drop(dlt.pipeline(**kwargs))
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

MIN_ETF = 50
_NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com/",
}


def _num(value):
    if value in (None, "", "-"):
        return None
    return float(str(value).replace(",", ""))


def _fetch_etfs():
    with httpx.Client(headers=_NSE_HEADERS, follow_redirects=True, timeout=30) as http:
        http.get("https://www.nseindia.com")
        response = http.get("https://www.nseindia.com/api/etf")
        response.raise_for_status()
        payload = response.json()
    stamp = payload.get("timestamp")
    as_of = (
        datetime.strptime(stamp, "%d-%b-%Y %H:%M:%S")
        .replace(tzinfo=ZoneInfo("Asia/Kolkata"))
        .astimezone(timezone.utc)
        if stamp
        else datetime.now(timezone.utc)
    )
    rows = []
    for item in payload.get("data") or []:
        symbol = item.get("symbol")
        if not symbol:
            continue
        qty = _num(item.get("qty"))
        rows.append(
            {
                "symbol": str(symbol),
                "underlying": item.get("assets"),
                "series": item.get("series"),
                "open": _num(item.get("open")),
                "high": _num(item.get("high")),
                "low": _num(item.get("low")),
                "prev_close": _num(item.get("prevClose")),
                "ltp": _num(item.get("ltP")),
                "change": _num(item.get("chn")),
                "pct_change": _num(item.get("per")),
                "volume": None if qty is None else int(qty),
                "traded_value": _num(item.get("trdVal")),
                "nav": _num(item.get("nav")),
                "week_high": _num(item.get("wkhi")),
                "week_low": _num(item.get("wklo")),
                "pct_change_30d": _num(item.get("perChange30d")),
                "pct_change_365d": _num(item.get("perChange365d")),
                "as_of": as_of,
                "source": "nse",
            }
        )
    return rows


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
        rows = _fetch_etfs()
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


_EQUITY_COLUMNS = {
    "symbol": {"data_type": "text"},
    "instrument_token": {"data_type": "bigint"},
    "bar_ts": {"data_type": "timestamp"},
    "open": {"data_type": "double"},
    "high": {"data_type": "double"},
    "low": {"data_type": "double"},
    "close": {"data_type": "double"},
    "volume": {"data_type": "bigint"},
    "interval": {"data_type": "text"},
    "source": {"data_type": "text"},
}


def _equity_batch_resource(rows):
    @dlt.resource(
        name="equity_price_volume",
        write_disposition="append",
        table_format="iceberg",
        columns=_EQUITY_COLUMNS,
    )
    def equity_price_volume():
        yield from rows

    return equity_price_volume


def _historical_data(kite, token, start, end):
    last = None
    for attempt in range(5):
        try:
            return kite.historical_data(token, start, end, "minute")
        except (ReadTimeout, RequestsConnectionError, TimeoutError) as exc:
            last = exc
            time.sleep(min(2**attempt, 16))
    raise last


async def _equity_candles(kite, instrument, start, end):
    token = instrument["instrument_token"]
    symbol = instrument["tradingsymbol"]
    candles = await asyncio.to_thread(_historical_data, kite, token, start, end)
    return [
        {
            "symbol": symbol,
            "instrument_token": int(token),
            "bar_ts": candle["date"],
            "open": float(candle["open"]),
            "high": float(candle["high"]),
            "low": float(candle["low"]),
            "close": float(candle["close"]),
            "volume": int(candle.get("volume") or 0),
            "interval": "minute",
            "source": "kite",
        }
        for candle in candles
    ]


async def _equity_batch_rows(kite, batch, start, end):
    chunks = await asyncio.gather(
        *(_equity_candles(kite, instrument, start, end) for instrument in batch)
    )
    rows = []
    for chunk in chunks:
        rows.extend(chunk)
    return rows


def _progress(msg: str) -> None:
    """Dagster compute logs capture stdout, not logging.info without a handler."""
    print(msg, flush=True)


def _run_equity_price_volume(dlt_run):
    from .utils.fetch_kite_access_token import kite_client

    kite = kite_client()
    instruments = [
        instrument
        for instrument in kite.instruments()
        if instrument.get("instrument_type", "").upper() == "EQ"
    ]
    if not instruments:
        raise RuntimeError("Kite returned no EQ instruments")

    total = len(instruments)
    _progress(f"equity_price_volume start: {total} EQ instruments (Kite 3/s, Iceberg every 350)")

    end = datetime.now()
    start = end - timedelta(days=60)
    kite_batch = 3
    iceberg_flush = 350
    pending = []
    pending_names = 0
    load_info = None
    for i in range(0, total, kite_batch):
        batch = instruments[i : i + kite_batch]
        t0 = time.monotonic()
        pending.extend(asyncio.run(_equity_batch_rows(kite, batch, start, end)))
        pending_names += len(batch)
        covered = i + len(batch)
        elapsed = time.monotonic() - t0
        if i + kite_batch < total and elapsed < 1:
            time.sleep(1 - elapsed)
        _progress(
            f"equity_price_volume covered {covered}/{total} ({100.0 * covered / total:.1f}%) "
            f"kite_batch={len(batch)} buffer_names={pending_names} buffer_rows={len(pending)}"
        )
        if pending_names >= iceberg_flush:
            if pending:
                _progress(
                    f"equity_price_volume iceberg append starting at {covered}/{total} "
                    f"({len(pending)} rows) — S3 folder appears after this load finishes"
                )
                load_info = dlt_run(_equity_batch_resource(pending))
                _progress(f"equity_price_volume iceberg append done at {covered}/{total}")
            pending = []
            pending_names = 0
    if pending:
        _progress(
            f"equity_price_volume iceberg append leftover starting {pending_names} names "
            f"({len(pending)} rows) covered {total}/{total}"
        )
        load_info = dlt_run(_equity_batch_resource(pending))
        _progress(f"equity_price_volume iceberg append leftover done {total}/{total}")
    if load_info is None:
        raise RuntimeError("Kite historical_data returned no EQ minute bars")
    _progress(f"equity_price_volume finished {total}/{total} instruments")
    return load_info


@dlt.source(name="equity_price_volume")
def _equity_price_volume():
    @dlt.resource(
        name="equity_price_volume",
        write_disposition="append",
        table_format="iceberg",
        columns=_EQUITY_COLUMNS,
    )
    def equity_price_volume():
        """Kite 1-minute EQ OHLCV. Fetch 3/s; Iceberg append every 350 names."""
        yield from ()

    return equity_price_volume


equity_price_volume = _equity_price_volume()

_DLT_PIPELINES.mkdir(parents=True, exist_ok=True)
equity_price_volume_pipeline = _bind_drop(
    dlt.pipeline(
        pipeline_name="equity_price_volume",
        destination=s3_iceberg,
        dataset_name=glue_database,
        pipelines_dir=str(_DLT_PIPELINES),
    )
)
_equity_dlt_run = equity_price_volume_pipeline.run


def _equity_pipeline_run(*args, **kwargs):
    try:
        equity_price_volume_pipeline.abort_packages()
    except Exception:
        try:
            equity_price_volume_pipeline.drop_pending_packages()
        except Exception:
            pass
    return _run_equity_price_volume(_equity_dlt_run)


equity_price_volume_pipeline.run = _equity_pipeline_run
