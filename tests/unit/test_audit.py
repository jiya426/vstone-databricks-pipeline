"""Unit tests for audit columns (used by every ingestion path)."""
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

from vstone_framework.audit import AUDIT_COLUMNS, with_audit_columns


def test_audit_columns_schema(spark):
    df = spark.createDataFrame([("2010-01-31", "10001")], "Date string, RegionName string")
    out = df.transform(with_audit_columns("copy_into_csv", "123", F.lit("/Volumes/x/a.csv")))
    expected = StructType([StructField("Date", StringType()), StructField("RegionName", StringType()),
                           StructField("load_dt", TimestampType(), False), StructField("source", StringType(), False),
                           StructField("source_file", StringType(), False), StructField("batch_id", StringType(), False)])
    assertSchemaEqual(out.schema, expected)
    assert out.columns[-4:] == AUDIT_COLUMNS


def test_audit_column_values(spark):
    df = spark.createDataFrame([("2010-01-31", "10001")], "Date string, RegionName string")
    out = df.transform(with_audit_columns("autoloader_json", 987, F.lit("/Volumes/x/a.json"))).drop("load_dt")
    expected = spark.createDataFrame([("2010-01-31", "10001", "autoloader_json", "/Volumes/x/a.json", "987")],
                                     "Date string, RegionName string, source string, source_file string, batch_id string")
    assertDataFrameEqual(out, expected)
    assert df.transform(with_audit_columns("pyspark_xml", "1", F.lit("f"))).filter("load_dt IS NULL").count() == 0


def test_unknown_source_rejected():
    with pytest.raises(ValueError, match="Unknown source"):
        with_audit_columns("ftp_csv", "1")
