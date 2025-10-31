from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "htx-xdata-job") -> SparkSession:
    """Create or get a local SparkSession."""
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    return spark
