# ============================================================================
# This script create some seed data in Iceberg for local Iceberg FDW testing.
#
# Namespace: docs_example
# Table: docs_example.bids
# ============================================================================

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    TimestampType,
    TimestamptzType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    StringType,
    ListType,
    MapType,
    NestedField,
    StructType,
    UUIDType,
)
import pyarrow as pa
from datetime import datetime, timezone
import uuid

catalog = load_catalog(
    "docs",
    **{
        "type": "rest",
        "uri": "http://iceberg-rest:8181",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.endpoint": "http://s3:8000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
    }
)

namespace = "docs_example"
tblname = f"{namespace}.bids"

schema = Schema(
    NestedField(field_id=1, name="datetime", field_type=TimestampType(), required=True),
    NestedField(field_id=2, name="symbol", field_type=StringType(), required=True),
    NestedField(field_id=3, name="bid", field_type=FloatType(), required=False),
    NestedField(field_id=4, name="ask", field_type=DoubleType(), required=False),
    NestedField(
        field_id=5,
        name="details",
        field_type=StructType(
            NestedField(
                field_id=54, name="created_by", field_type=StringType(), required=False
            ),
        ),
        required=False,
    ),
    NestedField(field_id=6, name="amt", field_type=DecimalType(32, 3), required=False),
    NestedField(field_id=7, name="dt", field_type=DateType(), required=False),
    NestedField(field_id=8, name="tstz", field_type=TimestamptzType(), required=False),
    NestedField(field_id=9, name="uid", field_type=UUIDType(), required=False),
    NestedField(field_id=10, name="bin", field_type=BinaryType(), required=False),
    NestedField(field_id=11, name="bcol", field_type=BooleanType(), required=False),
    NestedField(field_id=12, name="list", field_type=ListType(
        element_id=50,
        element_type=StringType(),
        required=False
    ), required=False),
    NestedField(field_id=13, name="icol", field_type=IntegerType(), required=False),
    NestedField(field_id=14, name="map", field_type=MapType(
        key_id=100,
        key_type=StringType(),
        value_id=102,
        value_type=StringType(),
    ), required=False),
    NestedField(field_id=15, name="lcol", field_type=LongType(), required=False),
    #NestedField(field_id=16, name="Upcol", field_type=StringType(), required=False),
    #NestedField(field_id=17, name="space col", field_type=StringType(), required=False),
)

partition_spec = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
    )
)

sort_order = SortOrder(SortField(source_id=2, transform=IdentityTransform()))

catalog.create_namespace_if_not_exists(namespace)

ns = catalog.list_namespaces()
tables = catalog.list_tables(namespace)

if catalog.table_exists(tblname):
    catalog.purge_table(tblname)
tbl = catalog.create_table(
    identifier=tblname,
    schema=schema,
    #location="s3://iceberg",
    partition_spec=partition_spec,
    sort_order=sort_order,
)
table = catalog.load_table(tblname)
data = table.scan().to_arrow()

df = pa.Table.from_pylist(
    [
        {
            "datetime": datetime.now() - timedelta(days=1),
            "symbol": "APL",
            "bid": 12.34, "ask": 54.32, "amt": 998,
            "tstz": datetime(2025, 5, 16, 12, 34, 56, tzinfo=ZoneInfo("Asia/Singapore")),
            "details": {"created_by": "alice"},
            "map": { "nn": "qq", "nn2": "pp" },
            "bcol": True,
        },
        {
            "datetime": datetime.now(timezone.utc),
            "symbol": "MCS",
            "bid": 33.44, "ask": 11.22, "dt": date(2025, 5, 16),
            "uid": uuid.UUID(bytes=bytes([0x42] * 16)).bytes,
            "bin": bytes([0x43] * 16),
            "list": ["xx", "yy"],
            "map": { "kk": "val", "kk2": "123.4" },
            "icol": 1234, "lcol": 5678,
            #"Upcol": "uppercase col name",
            #"space col": "space in col name",
        },
    ],
    schema=tbl.schema().as_arrow(),
)

tbl.overwrite(df)

data = tbl.scan().to_arrow()
print(data)
