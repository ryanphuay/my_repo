import os

from pyspark.sql import Row

from src.htx_xdata.main import main as job_main


def test_integration_end_to_end(spark, monkeypatch, tmp_dir):
    df_a = spark.createDataFrame(
        [
            Row(
                geographical_location_oid=1,
                video_camera_oid=10,
                detection_oid=100,
                item_name="umbrella",
                timestamp_detected=1111,
            ),
            Row(
                geographical_location_oid=1,
                video_camera_oid=10,
                detection_oid=100,
                item_name="umbrella",
                timestamp_detected=1112,
            ),
            Row(
                geographical_location_oid=1,
                video_camera_oid=11,
                detection_oid=101,
                item_name="car",
                timestamp_detected=1113,
            ),
            Row(
                geographical_location_oid=2,
                video_camera_oid=20,
                detection_oid=200,
                item_name="umbrella",
                timestamp_detected=1114,
            ),
        ]
    )
    input_a_path = os.path.join(tmp_dir, "dataset_a.parquet")
    df_a.write.mode("overwrite").parquet(input_a_path)

    df_b = spark.createDataFrame(
        [
            Row(geographical_location_oid=1, geographical_location="Downtown"),
            Row(geographical_location_oid=2, geographical_location="Suburb"),
        ]
    )
    input_b_path = os.path.join(tmp_dir, "dataset_b.parquet")
    df_b.write.mode("overwrite").parquet(input_b_path)

    output_path = os.path.join(tmp_dir, "output.parquet")

    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "--input-a", input_a_path,
            "--input-b", input_b_path,
            "--output", output_path,
            "--top-x", "2",
        ],
    )

    job_main()

    out_df = spark.read.parquet(output_path)
    rows = out_df.collect()
    assert len(rows) == 3
