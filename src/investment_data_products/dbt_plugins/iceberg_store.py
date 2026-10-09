import os
from typing import Any

import pyarrow as pa
from dbt.adapters.duckdb.plugins import BasePlugin
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchIcebergTableError,
    NoSuchPropertyException,
    NoSuchTableError,
)


def _catalog():
    warehouse = os.environ["S3_LAKE_BASE"].rstrip("/")
    return load_catalog(
        "glue",
        **{
            "type": "glue",
            "warehouse": warehouse,
            "client.region": os.environ["AWS_REGION"],
            "s3.region": os.environ["AWS_REGION"],
            "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
        },
    )


def write_iceberg_table(arrow_table: pa.Table, table_name: str) -> str:
    database = os.environ["GLUE_DATABASE"]
    warehouse = os.environ["S3_LAKE_BASE"].rstrip("/")
    identifier = f"{database}.{table_name}"
    location = f"{warehouse}/{database}/{table_name}"

    try:
        from dlt.common.libs.pyiceberg import ensure_iceberg_compatible_arrow_data

        arrow_table = ensure_iceberg_compatible_arrow_data(arrow_table)
    except Exception:
        pass

    catalog = _catalog()
    try:
        catalog.create_namespace(database)
    except NamespaceAlreadyExistsError:
        pass

    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        _create_and_append(catalog, identifier, arrow_table, location)
    except (NoSuchPropertyException, NoSuchIcebergTableError):
        # Glue already has this name, but the table is not Iceberg
        # (no table_type parameter). Replace that catalog entry.
        catalog.drop_table(identifier)
        _create_and_append(catalog, identifier, arrow_table, location)
    else:
        table.overwrite(arrow_table)
    return identifier


def _create_and_append(catalog: Any, identifier: str, arrow_table: pa.Table, location: str) -> None:
    table = catalog.create_table(
        identifier,
        schema=arrow_table.schema,
        location=location,
    )
    table.append(arrow_table)


class Plugin(BasePlugin):
    def configure_connection(self, conn: Any) -> None:
        def _write(qualified_name: str) -> str:
            table_name = qualified_name.replace('"', "").split(".")[-1]
            arrow_table = conn.sql(f"select * from {qualified_name}").arrow()
            return write_iceberg_table(arrow_table, table_name)

        conn.create_function("write_iceberg_relation", _write)
