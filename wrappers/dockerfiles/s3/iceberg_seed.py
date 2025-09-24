# ============================================================================
# This script create some seed data in Iceberg for local Iceberg FDW testing.
#
# Namespace: docs_example
# Table: docs_example.bids
# ============================================================================

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, HourTransform, MonthTransform, YearTransform, IdentityTransform
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    TimeType,
    TimestampType,
    TimestamptzType,
    TimestampNanoType,
    TimestamptzNanoType,
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

def create_bids_table(catalog, namespace):
    tblname = f"{namespace}.bids"
    schema = Schema(
        NestedField(field_id=1, name="datetime", field_type=TimestampType(), required=True),
        NestedField(field_id=2, name="symbol", field_type=StringType(), required=True),
        NestedField(field_id=3, name="bid", field_type=FloatType(), required=False),
        NestedField(field_id=4, name="ask", field_type=DoubleType(), required=False),
        NestedField(
            field_id=5,
            name="details",
            required=False,
            field_type=StructType(
                NestedField(
                    field_id=54, name="created_by", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=55, name="balance", field_type=FloatType(), required=False
                ),
                NestedField(
                    field_id=56, name="balance2", field_type=DoubleType(), required=False
                ),
                NestedField(
                    field_id=57, name="count", field_type=IntegerType(), required=False
                ),
                NestedField(
                    field_id=58, name="count2", field_type=LongType(), required=False
                ),
                NestedField(
                    field_id=59, name="valid", field_type=BooleanType(), required=False
                ),
            ),
        ),
        NestedField(field_id=6, name="amt", field_type=DecimalType(32, 3), required=False),
        NestedField(field_id=7, name="dt", field_type=DateType(), required=False),
        NestedField(field_id=8, name="tstz", field_type=TimestamptzType(), required=False),
        NestedField(field_id=9, name="uid", field_type=UUIDType(), required=False),
        NestedField(field_id=10, name="bin", field_type=BinaryType(), required=False),
        NestedField(field_id=11, name="bcol", field_type=BooleanType(), required=False),
        NestedField(field_id=12, name="list", field_type=ListType(
            element_id=50, element_type=StringType(), required=False
        ), required=False),
        NestedField(field_id=13, name="icol", field_type=IntegerType(), required=False),
        NestedField(field_id=14, name="map", field_type=MapType(
            key_id=100, key_type=StringType(), value_id=102, value_type=StringType(),
        ), required=False),
        NestedField(field_id=15, name="lcol", field_type=LongType(), required=False),
        #NestedField(field_id=16, name="Upcol", field_type=StringType(), required=False),
        #NestedField(field_id=17, name="space col", field_type=StringType(), required=False),
        NestedField(field_id=21, name="tcol", field_type=TimeType(), required=False),

        NestedField(field_id=22, name="map2", field_type=MapType(
            key_id=200, key_type=StringType(), value_id=202, value_type=FloatType(),
        ), required=False),
        NestedField(field_id=23, name="map3", field_type=MapType(
            key_id=204, key_type=StringType(), value_id=206, value_type=BooleanType(), value_required=False,
        ), required=False),
        NestedField(field_id=24, name="map4", field_type=MapType(
            key_id=208, key_type=StringType(), value_id=210, value_type=IntegerType(), value_required=False,
        ), required=False),
        NestedField(field_id=25, name="map5", field_type=MapType(
            key_id=212, key_type=StringType(), value_id=214, value_type=LongType(), value_required=False,
        ), required=False),
        NestedField(field_id=26, name="map6", field_type=MapType(
            key_id=216, key_type=StringType(), value_id=218, value_type=DoubleType(), value_required=False,
        ), required=False),

        NestedField(field_id=27, name="list2", field_type=ListType(
            element_id=300, element_type=LongType(), element_required=False, required=False
        ), required=False),
        NestedField(field_id=28, name="list3", field_type=ListType(
            element_id=302, element_type=FloatType(), element_required=False, required=False
        ), required=False),
        NestedField(field_id=29, name="list4", field_type=ListType(
            element_id=304, element_type=BooleanType(), element_required=False, required=False
        ), required=False),
        NestedField(field_id=30, name="list5", field_type=ListType(
            element_id=306, element_type=IntegerType(), element_required=False, required=False
        ), required=False),
        NestedField(field_id=31, name="list6", field_type=ListType(
            element_id=308, element_type=DoubleType(), element_required=False, required=False
        ), required=False),

        NestedField(field_id=32, name="pat_col_year", field_type=TimestampType(), required=False),
        NestedField(field_id=33, name="pat_col_month", field_type=DateType(), required=False),
        NestedField(field_id=34, name="pat_col_hour", field_type=TimestampType(), required=False),
        NestedField(field_id=35, name="pat_bcol", field_type=BooleanType(), required=False),
        NestedField(field_id=36, name="pat_icol", field_type=IntegerType(), required=False),
        NestedField(field_id=37, name="pat_lcol", field_type=LongType(), required=False),
        NestedField(field_id=38, name="pat_tcol", field_type=TimestampType(), required=False),

        identifier_field_ids=[1],
    )

    partition_spec = PartitionSpec(
        PartitionField(
            source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
        ),
        PartitionField(
            source_id=2, field_id=1002, transform=IdentityTransform(), name="symbol_ident"
        ),
        PartitionField(
            source_id=3, field_id=1004, transform=IdentityTransform(), name="bid_ident"
        ),
        PartitionField(
            source_id=4, field_id=1006, transform=IdentityTransform(), name="ask_ident"
        ),
        PartitionField(
            source_id=7, field_id=1008, transform=DayTransform(), name="dt_day"
        ),
        PartitionField(
            source_id=11, field_id=1010, transform=IdentityTransform(), name="bcol_ident"
        ),
        PartitionField(
            source_id=13, field_id=1012, transform=IdentityTransform(), name="icol_ident"
        ),
        PartitionField(
            source_id=32, field_id=1014, transform=YearTransform(), name="pat_year"
        ),
        PartitionField(
            source_id=33, field_id=1016, transform=MonthTransform(), name="pat_month"
        ),
        PartitionField(
            source_id=34, field_id=1018, transform=HourTransform(), name="pat_hour"
        ),
        PartitionField(
            source_id=35, field_id=1020, transform=IdentityTransform(), name="pat_bcol_ident"
        ),
        PartitionField(
            source_id=36, field_id=1022, transform=IdentityTransform(), name="pat_icol_ident"
        ),
        PartitionField(
            source_id=37, field_id=1024, transform=IdentityTransform(), name="pat_lcol_ident"
        ),
        PartitionField(
            source_id=38, field_id=1026, transform=MonthTransform(), name="pat_tcol_month"
        ),
    )

    sort_order = SortOrder(SortField(source_id=2, transform=IdentityTransform()))

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

    df = pa.Table.from_pylist(
        [
            {
                "datetime": datetime.now() - timedelta(days=1),
                "symbol": "APL",
                "bid": 12.34, "ask": 54.32, "amt": 998,
                "tstz": datetime(2025, 5, 16, 12, 34, 56, tzinfo=ZoneInfo("Asia/Singapore")),
                "details": {
                    "created_by": "alice",
                    "balance": 222.33,
                    "count": 42,
                    "valid": True,
                },
                "map": { "nn": "qq", "nn2": "pp" },
                "bcol": True,
                "pat_col_year": datetime.now() - timedelta(days=1),
                "pat_col_month": date(2025, 5, 16),
                "tcol": time.fromisoformat('04:23:01'),
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


def create_asks_table(catalog, namespace):
    tblname = f"{namespace}.asks"
    schema = Schema(
        NestedField(field_id=1, name="datetime", field_type=TimestampType(), required=True),
        NestedField(field_id=2, name="symbol", field_type=StringType(), required=True),
        NestedField(field_id=3, name="ask", field_type=DoubleType(), required=False),
        identifier_field_ids=[1],
    )

    sort_order = SortOrder(SortField(source_id=2, transform=IdentityTransform()))

    if catalog.table_exists(tblname):
        catalog.purge_table(tblname)
    tbl = catalog.create_table(
        identifier=tblname,
        schema=schema,
        #location="s3://iceberg",
        sort_order=sort_order,
    )
    table = catalog.load_table(tblname)

    df = pa.Table.from_pylist(
        [
            {
                "datetime": datetime.now() - timedelta(days=1),
                "symbol": "APL",
                "ask": 12.34,
            },
        ],
        schema=tbl.schema().as_arrow(),
    )

    tbl.overwrite(df)

    data = tbl.scan().to_arrow()
    print(data)


catalog.create_namespace_if_not_exists(namespace)
ns = catalog.list_namespaces()
tables = catalog.list_tables(namespace)

create_bids_table(catalog, namespace)
create_asks_table(catalog, namespace)
