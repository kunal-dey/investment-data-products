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

kite_pipeline = dlt.pipeline(
    pipeline_name="kite_mf_instruments",
    destination=s3_iceberg,
    dataset_name=glue_database,
)

kite_instruments_pipeline = dlt.pipeline(
    pipeline_name="kite_instruments",
    destination=s3_iceberg,
    dataset_name=glue_database,
)


if __name__ == "__main__":
    load_info = kite_instruments_pipeline.run(kite_instruments)
    print(load_info)
    print(f"Glue catalog: {glue_database}.instruments")
    print(f"Iceberg location: {s3_lake_base}/{glue_database}/instruments")
