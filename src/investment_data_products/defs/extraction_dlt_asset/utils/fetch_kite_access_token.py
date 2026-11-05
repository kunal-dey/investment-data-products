import json
import os
from datetime import datetime, time, timedelta
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import ClientError
from kiteconnect import KiteConnect

ACCESS_TOKEN_KEY = "kite/access_token.json"
IST = ZoneInfo("Asia/Kolkata")


def kite_client() -> KiteConnect:
    kite = KiteConnect(api_key=_api_key(), timeout=30)
    kite.set_access_token(_load_access_token())
    return kite


def _api_key() -> str:
    client = boto3.client("secretsmanager", region_name=os.environ["AWS_REGION"])
    secret = json.loads(client.get_secret_value(SecretId="credential_store")["SecretString"])
    key = secret.get("KITE_API_KEY")
    if not key:
        raise RuntimeError("credential_store secret is missing KITE_API_KEY")
    return key


def _load_access_token() -> str:
    bucket, prefix = _split_s3_uri(os.environ["S3_LAKE_BASE"])
    key = "/".join(part for part in (prefix, ACCESS_TOKEN_KEY) if part)
    try:
        body = boto3.client("s3", region_name=os.environ["AWS_REGION"]).get_object(
            Bucket=bucket, Key=key
        )["Body"].read()
        payload = json.loads(body)
        token = payload["access_token"]
        updated_at = datetime.fromisoformat(payload["updated_at"])
    except (ClientError, KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"Kite access token missing at s3://{bucket}/{key}") from exc
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=IST)
    expiry = datetime.combine(updated_at.astimezone(IST).date() + timedelta(days=1), time(6, 0), tzinfo=IST)
    if datetime.now(IST) >= expiry:
        raise RuntimeError("Kite access token expired (valid until 06:00 IST next day)")
    if not token:
        raise RuntimeError("Kite access token is empty")
    return token


def _split_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError("S3_LAKE_BASE must be an s3://bucket URI")
    return parsed.netloc, parsed.path.strip("/")
