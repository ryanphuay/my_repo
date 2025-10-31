from __future__ import annotations

import argparse
from dataclasses import dataclass


@dataclass
class JobConfig:
    input_a: str
    input_b: str
    output: str
    top_x: int = 10
    app_name: str = "htx-xdata-job"


def parse_args() -> JobConfig:
    desc = (
        "HTX xData – compute top X items per geographical location "
        "(RDD only)."
    )
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        "--input-a",
        required=True,
        help="Path to Dataset A parquet",
    )
    parser.add_argument(
        "--input-b",
        required=True,
        help="Path to Dataset B parquet",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output path for parquet result",
    )
    parser.add_argument(
        "--top-x",
        type=int,
        default=10,
        help="Top X items to keep",
    )
    parser.add_argument(
        "--app-name",
        default="htx-xdata-job",
        help="Spark app name",
    )

    args = parser.parse_args()
    return JobConfig(
        input_a=args.input_a,
        input_b=args.input_b,
        output=args.output,
        top_x=args.top_x,
        app_name=args.app_name,
    )
