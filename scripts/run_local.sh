#!/usr/bin/env bash
set -euo pipefail

spark-submit   --master local[2]   src/htx_xdata/main.py   --input-a ./data/dataset_a.parquet   --input-b ./data/dataset_b.parquet   --output ./data/output.parquet   --top-x 10
