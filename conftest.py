"""Shared pytest fixtures: one local SparkSession for the whole test run."""
import os
import sys

import pytest
from pyspark.sql import SparkSession

# Driver and Python workers must use the same interpreter
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
os.environ.setdefault("PYARROW_IGNORE_TIMEZONE", "1")


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder.master("local[1]").appName("vstone-unit-tests")
               .config("spark.sql.shuffle.partitions", "1")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.session.timeZone", "UTC")
               .getOrCreate())
    yield session
    session.stop()
