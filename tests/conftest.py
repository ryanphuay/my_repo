import shutil
import tempfile

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("htx-xdata-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def tmp_dir():
    d = tempfile.mkdtemp(prefix="htx-xdata-")
    try:
        yield d
    finally:
        shutil.rmtree(d, ignore_errors=True)
