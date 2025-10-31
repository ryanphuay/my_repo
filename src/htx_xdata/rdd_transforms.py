from __future__ import annotations

from typing import Iterable, List, Tuple

from pyspark import RDD
from pyspark.sql import Row, SparkSession

DatasetARecord = Tuple[int, int, int, str, int]
GeoItemKey = Tuple[int, str]


def deduplicate_by_detection_id(
    dataset_a_rdd: RDD[DatasetARecord],
) -> RDD[DatasetARecord]:
    keyed = dataset_a_rdd.map(lambda r: (r[2], r))
    deduped = keyed.reduceByKey(lambda r1, _: r1).values()
    return deduped


def count_items_per_geo(
    deduped_rdd: RDD[DatasetARecord],
) -> RDD[Tuple[int, Tuple[str, int]]]:
    pair_counts = (
        deduped_rdd
        .map(lambda r: ((r[0], r[3]), 1))
        .reduceByKey(lambda a, b: a + b)
    )
    return pair_counts.map(lambda kv: (kv[0][0], (kv[0][1], kv[1])))


def _merge_top_x(
    existing: List[Tuple[str, int]],
    new_item: Tuple[str, int],
    top_x: int,
) -> List[Tuple[str, int]]:
    existing.append(new_item)
    existing.sort(key=lambda x: (-x[1], x[0]))
    return existing[:top_x]


def compute_top_x_per_geo(
    counts_rdd: RDD[Tuple[int, Tuple[str, int]]],
    top_x: int,
) -> RDD[Tuple[int, List[Tuple[str, int]]]]:
    def create_combiner(v: Tuple[str, int]) -> List[Tuple[str, int]]:
        return [v]

    def merge_value(
        comb: List[Tuple[str, int]],
        v: Tuple[str, int],
    ) -> List[Tuple[str, int]]:
        return _merge_top_x(comb, v, top_x)

    def merge_combiners(
        c1: List[Tuple[str, int]],
        c2: List[Tuple[str, int]],
    ) -> List[Tuple[str, int]]:
        combined = c1 + c2
        combined.sort(key=lambda x: (-x[1], x[0]))
        return combined[:top_x]

    return counts_rdd.combineByKey(
        create_combiner,
        merge_value,
        merge_combiners,
    )


def to_output_rows(
    spark: SparkSession,
    top_x_rdd: RDD[Tuple[int, List[Tuple[str, int]]]],
) -> RDD[Row]:
    def expand(record: Tuple[int, List[Tuple[str, int]]]) -> Iterable[Row]:
        geo_oid, items = record
        for idx, (item_name, _) in enumerate(items, start=1):
            yield Row(
                geographical_location_oid=geo_oid,
                item_rank=idx,
                item_name=item_name,
            )

    return top_x_rdd.flatMap(expand)
