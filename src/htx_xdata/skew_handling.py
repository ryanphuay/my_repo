from __future__ import annotations

import random
from typing import Tuple

from pyspark import RDD

from .rdd_transforms import DatasetARecord


def salt_hot_key_counts(
    deduped_rdd: RDD[DatasetARecord],
    hot_key: int,
    num_salts: int = 20,
):
    def add_salt(record: DatasetARecord) -> Tuple[Tuple[int, int, str], int]:
        geo, _cam, _det, item_name, _ts = record
        if geo == hot_key:
            salt = random.randint(0, num_salts - 1)
            return ((geo, salt, item_name), 1)
        else:
            return ((geo, 0, item_name), 1)

    salted = deduped_rdd.map(add_salt).reduceByKey(lambda a, b: a + b)

    unsalted = (
        salted
        .map(lambda kv: ((kv[0][0], kv[0][2]), kv[1]))
        .reduceByKey(lambda a, b: a + b)
    )

    return unsalted
