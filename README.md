# HTX xData Technical Test – PySpark (RDD-only)

The goal of the exercise is to process camera detection data (a large parquet dataset) together with a small reference/location dataset, and produce a ranked list of the most frequently detected items per geographical location. The twist is that the main transformations have to be done with **RDDs** instead of the usual Spark DataFrame API.

## What this project does

- **Reads Dataset A (parquet)** – a large table of detections with:
  - `geographical_location_oid`
  - `video_camera_oid`
  - `detection_oid`
  - `item_name`
  - `timestamp_detected`
  - Note: the same `detection_oid` can appear more than once.

- **Reads Dataset B (parquet)** – a small lookup (≈10k rows) that maps:
  - `geographical_location_oid` → `geographical_location`

- **Deduplicates** Dataset A **by `detection_oid`** so that the same detection is only counted once.

- **Counts** how many times each `item_name` appears **per `geographical_location_oid`**.

- **Keeps only the Top X items per location** (X is configurable, defaults to 10).

- **Writes the result** as parquet with the following columns:

  - `geographical_location_oid` (bigint)
  - `item_rank` (1 = most popular in that location)
  - `item_name` (string)

So the final output looks like: “for location 101, item A is #1, item B is #2, …”.

## Why RDDs?

The problem statement specifically asks to do the core data manipulation using RDD operations. Because of that, the code follows this pattern:

1. Use DataFrame **only** to read parquet (since it’s convenient and stable).
2. Convert the DataFrame to an RDD of simple Python tuples.
3. Do **all** the business logic in RDD:
   - deduplication (keyed by `detection_oid`)
   - counting
   - top-N per key
4. Convert back to a DataFrame right at the end to write parquet.

This keeps the solution close to the requirement while still being practical.

## How to run it

You need Python, PySpark, and Java (since Spark needs a JVM).

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

spark-submit \
  --master local[2] \
  src/htx_xdata/main.py \
  --input-a /path/to/dataset_a.parquet \
  --input-b /path/to/dataset_b.parquet \
  --output /path/to/output.parquet \
  --top-x 10
```

- `--input-a` → your big detection parquet
- `--input-b` → your small location parquet
- `--output` → where to put the result parquet
- `--top-x` → how many items per location to keep

## Project layout

```text
src/htx_xdata/
  config.py        # CLI args
  spark.py         # SparkSession builder
  rdd_transforms.py  # dedup, count, top-X (all RDD-based)
  skew_handling.py # example of salting a hot key to reduce skew
  main.py          # wiring everything together
tests/
  ...              # pytest unit + integration tests
docs/
  sorting.md       # notes on Spark sorting / broadcast join
  design.md        # notes on a streaming/data-architecture version
scripts/
  run_local.sh     # quick local spark-submit
```

## Notes and assumptions

- I deduplicate **globally** by `detection_oid` (i.e. if it appears twice anywhere in Dataset A, I keep only one).
- Dataset B is small enough to be broadcast if we actually want the human-readable location name in the output.
- Output parquet is written with `mode("overwrite")` so repeated runs don’t fail.
- The tests use `pyspark` and will fail to start if Java isn’t available – that’s expected in a Python-only environment.

## Running checks

```bash
flake8 src tests
pytest
```

## Where to look first

If you are reviewing this, the main things to read are:

1. `src/htx_xdata/rdd_transforms.py` – core logic, shortest to understand
2. `src/htx_xdata/main.py` – how it’s glued together
3. `tests/test_rdd_transforms.py` – shows the intended behaviour in small data
