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
    # Apache Arrow (pandas UDFs) needs this on Java 17+; Databricks sets it for you
    java_opts = "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true"
    session = (SparkSession.builder.master("local[1]").appName("vstone-unit-tests")
               .config("spark.driver.memory", "3g")          # Databricks drivers have far more; local default is 1g
               .config("spark.driver.extraJavaOptions", java_opts)
               .config("spark.executor.extraJavaOptions", java_opts)
               .config("spark.sql.shuffle.partitions", "1")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.session.timeZone", "UTC")
               .getOrCreate())
    yield session
    session.stop()
