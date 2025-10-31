from src.htx_xdata import rdd_transforms as rt


def test_deduplicate_by_detection_id(spark):
    data = [
        (1, 10, 100, "umbrella", 1111),
        (1, 10, 100, "umbrella", 1112),
        (1, 11, 101, "car", 1113),
        (2, 20, 200, "umbrella", 1114),
    ]
    rdd = spark.sparkContext.parallelize(data)
    deduped = rt.deduplicate_by_detection_id(rdd).collect()
    assert len(deduped) == 3
    det_ids = sorted([row[2] for row in deduped])
    assert det_ids == [100, 101, 200]


def test_count_and_top_x(spark):
    data = [
        (1, 10, 100, "umbrella", 1111),
        (1, 11, 101, "car", 1113),
        (1, 12, 102, "car", 1114),
        (1, 13, 103, "person", 1115),
        (1, 14, 104, "car", 1116),
        (2, 20, 200, "umbrella", 1117),
    ]
    rdd = spark.sparkContext.parallelize(data)
    deduped = rt.deduplicate_by_detection_id(rdd)
    counts = rt.count_items_per_geo(deduped)
    top_rdd = rt.compute_top_x_per_geo(counts, top_x=2)
    result = dict(top_rdd.collect())

    geo1 = result[1]
    assert geo1[0][0] == "car"
    assert geo1[0][1] == 3

    geo2 = result[2]
    assert geo2[0][0] == "umbrella"
    assert geo2[0][1] == 1
