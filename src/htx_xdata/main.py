from __future__ import annotations

from pyspark.sql import SparkSession

from .config import parse_args
from .spark import get_spark
from .rdd_transforms import (
    deduplicate_by_detection_id,
    count_items_per_geo,
    compute_top_x_per_geo,
    to_output_rows,
)


def main() -> None:
    cfg = parse_args()
    spark: SparkSession = get_spark(cfg.app_name)

    df_a = spark.read.parquet(cfg.input_a)
    df_b = spark.read.parquet(cfg.input_b)
    _ = df_b.count()

    a_rdd = df_a.rdd.map(
        lambda row: (
            int(row["geographical_location_oid"]),
            int(row["video_camera_oid"]),
            int(row["detection_oid"]),
            row["item_name"],
            int(row["timestamp_detected"]),
        )
    )

    deduped_rdd = deduplicate_by_detection_id(a_rdd)
    counts_rdd = count_items_per_geo(deduped_rdd)
    top_x_rdd = compute_top_x_per_geo(counts_rdd, cfg.top_x)

    output_rdd = to_output_rows(spark, top_x_rdd)
    output_df = spark.createDataFrame(output_rdd)
    output_df.write.mode("overwrite").parquet(cfg.output)

    spark.stop()


if __name__ == "__main__":
    main()
