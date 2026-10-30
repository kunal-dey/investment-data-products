from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import httpx

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
IST = ZoneInfo("Asia/Kolkata")
LAYER1 = (
    ("1 Finance Macroeconomic Index", "1-finance-macroeconomic-index"),
    ("Services Sector Activity Index", "services-sector-activity-index"),
    ("Industrial Sector Performance Index", "industrial-sector-performance-index"),
    ("Agriculture Output Index", "agriculture-output-index"),
    ("Consumer Inflation Index", "consumer-inflation-index"),
    ("Equity Market Optimism Index", "equity-market-optimism-index"),
    ("Global Economic Impact Index", "global-economic-impact-index"),
    ("Financial Sector Soundness Index", "financial-sector-soundness-index"),
    ("Interest Rate Outlook Index", "interest-rate-outlook-index"),
)
LAYER2 = (
    ("Real GDP Growth", "GROWTH"),
    ("GVA - Overall", "GROWTH"),
    ("GVA - Industry", "GROWTH"),
    ("GVA - Services", "GROWTH"),
    ("IIP- Overall", "GROWTH"),
    ("IIP- Manufacturing", "GROWTH"),
    ("IIP- 8 Core Industries", "GROWTH"),
    ("HSBC Manufacturing PMI", "GROWTH"),
    ("HSBC Services PMI", "GROWTH"),
    ("GST Collections", "GROWTH"),
    ("CPI Inflation – Headline", "INFLATION"),
    ("CPI Inflation – Core", "INFLATION"),
    ("CPI Inflation – Food", "INFLATION"),
    ("WPI Inflation - Overall", "INFLATION"),
    ("WPI Inflation - Food articles", "INFLATION"),
    ("WPI Inflation - Fuel & Power", "INFLATION"),
    ("WPI Inflation - Manufacturing", "INFLATION"),
    ("Repo Rate", "RATES"),
    ("Cash Reserve Ratio (CRR)", "RATES"),
    ("10Y G-Sec Yield", "RATES"),
    ("G-Sec Yield Curve", "RATES"),
    ("Real Interest Rate", "RATES"),
    ("RBI Liquidity (Net LAF)", "RATES"),
    ("MCLR", "RATES"),
    ("WALR", "RATES"),
    ("Bank Credit Growth", "BANKING"),
    ("Bank Credit - Industry", "BANKING"),
    ("Bank Credit - Services", "BANKING"),
    ("Bank Credit - Personal Loans", "BANKING"),
    ("Aggregate Bank Deposits", "BANKING"),
    ("Credit-Deposit Ratio", "BANKING"),
    ("NIFTY Bank", "BANKING"),
    ("USD/INR Exchange Rate", "EXTERNAL"),
    ("Crude Oil Price (Indian Basket)", "EXTERNAL"),
    ("Foreign Exchange Reserves", "EXTERNAL"),
    ("Import Cover (Months)", "EXTERNAL"),
    ("Current Account Deficit (% GDP)", "EXTERNAL"),
    ("Merchandise Trade Balance", "EXTERNAL"),
    ("Foreign Portfolio Investments (FPIs) - Equity", "EXTERNAL"),
    ("Foreign Portfolio Investments (FPIs) - Debt", "EXTERNAL"),
    ("DIIs Investment - Equity", "EXTERNAL"),
    ("Gross Foreign Direct Investment (FDI)", "EXTERNAL"),
    ("NIFTY 50", "VALUATION"),
    ("NIFTY 50 P/E Ratio", "VALUATION"),
    ("NIFTY Bank P/E Ratio", "VALUATION"),
    ("NIFTY Bank P/B Ratio", "VALUATION"),
    ("NIFTY IT P/E Ratio", "VALUATION"),
    ("NIFTY Auto P/E Ratio", "VALUATION"),
    ("NIFTY FMCG P/E Ratio", "VALUATION"),
    ("NIFTY Metal P/E Ratio", "VALUATION"),
    ("NIFTY Pharma P/E Ratio", "VALUATION"),
    ("NIFTY Oil And Gas P/E Ratio", "VALUATION"),
    ("NIFTY Midcap 150 P/E Ratio", "VALUATION"),
    ("NIFTY Smallcap 250 P/E Ratio", "VALUATION"),
    ("Domestic Passenger Car Sales", "CONSUMPTION"),
    ("Domestic Two-Wheeler Sales", "CONSUMPTION"),
    ("Private Consumption (PFCE)", "CONSUMPTION"),
    ("Air Passenger Traffic - Domestic", "CONSUMPTION"),
    ("Southwest Monsoon Deviation (SWMD)", "RURAL"),
    ("Annual Rainfall (% of LPA)", "RURAL"),
    ("Reservoir Level (vs 10Y Avg)", "RURAL"),
    ("Domestic Tractor Sales", "RURAL"),
    ("Gold Price", "COMMODITIES"),
    ("Silver Price", "COMMODITIES"),
    ("Steel Production", "COMMODITIES"),
    ("Cement Production", "COMMODITIES"),
    ("Petroleum Consumption", "COMMODITIES"),
    ("Government Fiscal Deficit", "FISCAL"),
    ("Debt-to-GDP Ratio", "FISCAL"),
    ("Government Capital Expenditure", "FISCAL"),
)
EXPECTED_NAMES = frozenset(name for name, _ in LAYER1) | frozenset(name for name, _ in LAYER2)


def parse_pulse_time(text: str):
    try:
        local = datetime.strptime(text.strip(), "%I:%M %p, %d %b %Y")
    except (ValueError, AttributeError):
        return None
    return local.replace(tzinfo=IST).astimezone(timezone.utc)


def parse_pulse(html: str):
    from selectolax.lexbor import LexborHTMLParser as HTMLParser

    rows = []
    for card in HTMLParser(html).css("#news li.item"):
        link = card.css_first("h2.title a")
        if link is None or not link.attributes.get("data-id"):
            continue
        date = card.css_first("span.date")
        published_text = (date.attributes.get("title") if date else "") or ""
        desc = card.css_first(".desc")
        related = []
        for item in card.css("ul.similar li"):
            related.append(item.text(separator=" ", strip=True))
        rows.append(
            {
                "pulse_id": link.attributes["data-id"],
                "headline": link.text(strip=True),
                "summary": desc.text(strip=True) if desc else None,
                "published_at": parse_pulse_time(published_text),
                "related_headlines": " | ".join(part for part in related if part),
                "related_count": len(related),
            }
        )
    return rows


def _epoch(value) -> datetime:
    stamp = float(value)
    if stamp > 10**12:
        stamp /= 1000
    return datetime.fromtimestamp(stamp, timezone.utc)


def _day(value):
    if not value:
        return None
    text = str(value)[:10]
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def index_row(payload: dict, name: str):
    points = payload.get("data") or []
    if not points or points[-1].get("index_value") is None:
        return None
    return {
        "name": name,
        "group": "REGIME",
        "value": float(points[-1]["index_value"]),
        "unit": "index",
        "as_of": _epoch(points[-1]["index_date"]),
        "source": "indiamacroindicators",
    }


def _indicator_key(text: str) -> str:
    return "".join(ch.lower() for ch in text if ch.isalnum())


def series_row(items, name: str, group: str):
    want = _indicator_key(name)
    match = next((item for item in items if item.get("indicator_name") == name), None)
    if match is None:
        match = next(
            (
                item
                for item in items
                if want and want in _indicator_key(item.get("indicator_name") or "")
            ),
            None,
        )
    if not match or match.get("latest_value") in (None, ""):
        return None
    return {
        "name": name,
        "group": group,
        "value": float(str(match["latest_value"]).replace(",", "")),
        "unit": match.get("unit") or "",
        "as_of": _day(match.get("latest_value_date")),
        "source": "indiamacroindicators",
    }


def driver(name, value, unit, as_of, source):
    return {
        "name": name,
        "group": "SHORT_HORIZON",
        "value": float(value),
        "unit": unit,
        "as_of": as_of,
        "source": source,
    }


def gift_row(payload: dict):
    rows = [
        row
        for row in payload.get("data") or []
        if row.get("SYMBOL") == "NIFTY" and row.get("INSTRUMENTTYPE") == "FUTIDX" and row.get("LASTPRICE")
    ]
    if not rows:
        return None
    rows.sort(key=lambda row: datetime.strptime(row["EXPIRYDATE"], "%d-%b-%Y"))
    row = rows[0]
    as_of = datetime.strptime(row["TIMESTMP"], "%d-%b-%Y %H:%M:%S").replace(tzinfo=IST).astimezone(timezone.utc)
    return driver("GIFT_NIFTY", row["LASTPRICE"], "points", as_of, "nseix")


def _flow_records(payload):
    if isinstance(payload, list):
        return payload
    records = []
    for key, value in (payload or {}).items():
        if isinstance(value, list):
            for item in value:
                item = dict(item)
                item.setdefault("category", key)
                records.append(item)
    return records


def flow_rows(payload, today):
    parsed = []
    for item in _flow_records(payload):
        category = str(item.get("category") or "")
        net = item.get("netValue") or item.get("net")
        if net in (None, "") or not item.get("date"):
            continue
        upper = category.upper()
        if "DII" in upper:
            name = "DII_CASH_NET"
        elif "FII" in upper or "FPI" in upper:
            name = "FII_CASH_NET"
        else:
            continue
        parsed.append(
            (
                datetime.strptime(item["date"], "%d-%b-%Y").date(),
                name,
                float(str(net).replace(",", "")),
            )
        )
    if not parsed:
        return []
    dates = {item[0] for item in parsed}
    chosen = max((day for day in dates if day < today), default=max(dates))
    as_of = datetime(chosen.year, chosen.month, chosen.day, tzinfo=IST).astimezone(timezone.utc)
    return [driver(name, value, "inr_crore", as_of, "nse") for day, name, value in parsed if day == chosen]


def vix_from_indices(payload):
    data = payload.get("data") if isinstance(payload, dict) else payload
    for row in data or []:
        label = str(row.get("index") or row.get("indexSymbol") or row.get("name") or "")
        if label.upper().replace(" ", "") != "INDIAVIX":
            continue
        value = row.get("previousClose") or row.get("prevClose") or row.get("last")
        if value in (None, ""):
            return None
        return float(str(value).replace(",", ""))
    return None


def client():
    return httpx.Client(headers={"User-Agent": UA}, follow_redirects=True, timeout=30)


def fetch_macro(http):
    rows = []
    for name, slug in LAYER1:
        try:
            payload = http.get(f"https://admin.indiamacroindicators.co.in/api/indices/{slug}").json()
            row = index_row(payload, name)
        except Exception:
            row = None
        if row:
            rows.append(row)
    for name, group in LAYER2:
        try:
            payload = http.get(
                "https://admin.indiamacroindicators.co.in/api/economic-dashboard/search",
                params={"q": name},
            ).json()
            row = series_row(payload, name, group)
        except Exception:
            row = None
        if row:
            rows.append(row)
    return rows


def _nse_json(path: str):
    headers = {"User-Agent": UA, "Accept": "application/json", "Referer": "https://www.nseindia.com/"}
    with httpx.Client(headers=headers, follow_redirects=True, timeout=30) as http:
        http.get("https://www.nseindia.com", headers={"User-Agent": UA, "Accept": "text/html"})
        response = http.get("https://www.nseindia.com" + path)
        response.raise_for_status()
        return response.json()


def _num(value):
    if value in (None, "", "-"):
        return None
    return float(str(value).replace(",", ""))


def fetch_etfs():
    """NSE ETF market watch (`/api/etf`). One row per listed ETF."""
    payload = _nse_json("/api/etf")
    stamp = payload.get("timestamp")
    as_of = (
        datetime.strptime(stamp, "%d-%b-%Y %H:%M:%S").replace(tzinfo=IST).astimezone(timezone.utc)
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


def _yahoo_last(symbol: str):
    import yfinance as yf

    info = yf.Ticker(symbol).fast_info
    value = getattr(info, "last_price", None) or info["lastPrice"]
    return float(value), datetime.now(timezone.utc)


def _yahoo_close(symbol: str):
    import yfinance as yf

    history = yf.Ticker(symbol).history(period="10d")
    stamp = history.index[-1].to_pydatetime()
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return float(history["Close"].iloc[-1]), stamp.astimezone(timezone.utc)


def fetch_drivers(http, today=None):
    today = today or datetime.now(IST).date()
    rows = []
    try:
        rows.append(gift_row(http.get(
            "https://www.nseix.com/api/derivatives-watch",
            params={"inst_type1": "IDX", "inst_type2": "STK", "inst_type3": "CBI"},
        ).json()))
    except Exception:
        pass
    try:
        rows.extend(flow_rows(_nse_json("/api/fiidiiTradeReact"), today))
    except Exception:
        pass
    try:
        value = vix_from_indices(_nse_json("/api/allIndices"))
        if value is not None:
            rows.append(driver("INDIA_VIX", value, "percent", datetime.now(timezone.utc), "nse"))
        else:
            raise ValueError("missing india vix")
    except Exception:
        try:
            value, as_of = _yahoo_close("^INDIAVIX")
            rows.append(driver("INDIA_VIX", value, "percent", as_of, "yahoo"))
        except Exception:
            pass
    quotes = (
        ("BRENT", "BZ=F", "usd_per_barrel", True),
        ("US_10Y", "^TNX", "percent", True),
        ("SP500", "^GSPC", "points", False),
        ("NASDAQ", "^IXIC", "points", False),
    )
    for name, symbol, unit, last in quotes:
        try:
            value, as_of = _yahoo_last(symbol) if last else _yahoo_close(symbol)
            if name == "US_10Y":
                value /= 10
            rows.append(driver(name, value, unit, as_of, "yahoo"))
        except Exception:
            continue
    return [row for row in rows if row]


def fetch_all():
    with client() as http:
        page = http.get("https://pulse.zerodha.com/").text
        return parse_pulse(page), fetch_macro(http), fetch_drivers(http)
