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


def _as_table(data: Any) -> pa.Table:
    if isinstance(data, pa.Table):
        return data
    if hasattr(data, "read_all"):
        return data.read_all()
    return pa.table(data)


def _without_null_types(arrow_table: pa.Table) -> pa.Table:
    """Iceberg v2 cannot store pa.null(); all-None columns become string."""
    fields = []
    arrays = []
    for field in arrow_table.schema:
        column = arrow_table.column(field.name)
        if pa.types.is_null(field.type):
            column = pa.nulls(arrow_table.num_rows, type=pa.string())
            field = pa.field(field.name, pa.string(), nullable=True)
        fields.append(field)
        arrays.append(column)
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def write_iceberg_table(arrow_table: pa.Table, table_name: str) -> str:
    database = os.environ["GLUE_DATABASE"]
    warehouse = os.environ["S3_LAKE_BASE"].rstrip("/")
    identifier = f"{database}.{table_name}"
    location = f"{warehouse}/{database}/{table_name}"
    arrow_table = _as_table(arrow_table)

    try:
        from dlt.common.libs.pyiceberg import ensure_iceberg_compatible_arrow_data

        arrow_table = _as_table(ensure_iceberg_compatible_arrow_data(arrow_table))
    except Exception:
        pass
    arrow_table = _without_null_types(arrow_table)

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
    def store(self, target_config: Any) -> None:
        from dbt.adapters.duckdb.connections import DuckDBConnectionManager

        relation = target_config.relation
        catalog = os.path.splitext(os.path.basename(getattr(self.creds, "path", "") or ""))[0]
        catalog = catalog if catalog and catalog != ":memory:" else "memory"
        schema = relation.schema or "main"
        ident = relation.identifier
        env = DuckDBConnectionManager.env()
        conn = env.conn if getattr(env, "conn", None) is not None else env.handle().cursor()
        arrow = conn.execute(
            f'select * from "{catalog}"."{schema}"."{ident}"'
        ).fetch_arrow_table()
        write_iceberg_table(arrow, ident)
