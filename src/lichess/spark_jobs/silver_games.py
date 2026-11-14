"""Bronze month -> silver games, with data-quality checks that fail the job.

    JAVA_HOME=/opt/homebrew/opt/openjdk@17 uv run python -m lichess.spark_jobs.silver_games --month 2013-02

Writes `silver_root/month=YYYY-MM/` with dynamic partition overwrite, so only that
month is replaced (D-005). Exit code 3 means a data-quality failure, which a retry
won't fix; any other non-zero exit (JVM crash, OOM) may be worth retrying.
"""

import argparse
import json
import math
import sys
import time
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# Spark's default 200 shuffle partitions gave 200 files of ~200 KB for 2013-02. Size
# the file count from the row count instead; phase 5 measures the right target (D-017).
ROWS_PER_FILE = 500_000
EXIT_DATA_ERROR = 3


class DataQualityError(RuntimeError):
    pass


def spark_session(
    app_name: str = "silver_games", master: str = "local[*]"
) -> SparkSession:
    return (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


def tc_bucket(time_control: Column, base: Column, inc: Column) -> Column:
    """Lichess speed from TimeControl: estimated seconds = base + 40 * increment (D-011)."""
    est = base + 40 * inc
    return (
        F.when(time_control == "-", "correspondence")
        .when(est < 29, "ultrabullet")
        .when(est < 179, "bullet")
        .when(est < 479, "blitz")
        .when(est < 1499, "rapid")
        .when(est.isNotNull(), "classical")
    )


def opening_family(opening: Column) -> Column:
    # "Sicilian Defense: Najdorf Variation" -> "Sicilian Defense"; Lichess also numbers
    # sibling lines with a "#N" suffix ("Philidor Defense #3"), which isn't a family.
    head = F.trim(F.split(opening, ":", 2).getItem(0))
    return F.regexp_replace(head, r"\s+#\d+$", "")


def add_columns(bronze: DataFrame, month: str) -> DataFrame:
    avg_elo = (F.col("white_elo") + F.col("black_elo")) / 2
    return bronze.select(
        "*",
        (F.floor(avg_elo / 100) * 100).cast("int").alias("rating_band"),
        tc_bucket(
            F.col("time_control"), F.col("base_seconds"), F.col("increment_seconds")
        ).alias("tc_bucket"),
        opening_family(F.col("opening")).alias("opening_family"),
        F.when(F.col("result") == "1-0", 1.0)
        .when(F.col("result") == "0-1", 0.0)
        .when(F.col("result") == "1/2-1/2", 0.5)
        .alias("white_score"),
        ((F.col("termination") != "Abandoned") & (F.col("ply_count") >= 4)).alias(
            "is_decisive_play"
        ),
        F.lit(month).alias("month"),
    )


def dedup(df: DataFrame) -> DataFrame:
    # Deterministic winner so a rerun keeps the same row when duplicates differ.
    w = Window.partitionBy("game_id").orderBy("utc_datetime", "movetext")
    return (
        df.where(F.col("game_id").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn")
    )


def build_silver(bronze: DataFrame, month: str) -> DataFrame:
    return add_columns(dedup(bronze), month)


def check_quality(silver: DataFrame, expected_rows: int) -> dict[str, int]:
    row = silver.agg(
        F.count("*").alias("rows"),
        F.countDistinct("game_id").alias("distinct_ids"),
        F.sum(
            (F.col("tc_bucket").isNull() & (F.col("time_control") != "-")).cast("int")
        ).alias("null_tc_bucket"),
    ).first()
    rows, distinct_ids, null_tc = (
        row["rows"],
        row["distinct_ids"],
        row["null_tc_bucket"] or 0,
    )
    failures = []
    if rows != distinct_ids:
        failures.append(f"game_id not unique: {rows} rows, {distinct_ids} distinct")
    if rows != expected_rows:
        failures.append(
            f"row count {rows} != bronze minus dropped rows {expected_rows}"
        )
    if null_tc:
        failures.append(f"{null_tc} non-correspondence games with null tc_bucket")
    if failures:
        raise DataQualityError("; ".join(failures))
    return {"rows": rows, "distinct_ids": distinct_ids}


def run(spark: SparkSession, month: str, bronze_root: Path, silver_root: Path) -> dict:
    started = time.monotonic()
    bronze_path = bronze_root / f"month={month}"
    if not bronze_path.is_dir():
        raise FileNotFoundError(f"no bronze partition at {bronze_path}")
    bronze = spark.read.parquet(str(bronze_path))

    # Counted on bronze independently of the dedup code path, so the DQ check compares
    # two different computations rather than one number with itself.
    b = bronze.agg(
        F.count("*").alias("rows"),
        F.sum(F.col("game_id").isNull().cast("int")).alias("null_ids"),
        F.countDistinct("game_id").alias("distinct_ids"),
    ).first()
    bronze_rows, null_ids, expected = b["rows"], b["null_ids"] or 0, b["distinct_ids"]

    files = max(1, math.ceil(expected / ROWS_PER_FILE))
    silver = build_silver(bronze, month).repartition(files, "game_id").cache()
    try:
        check_quality(silver, expected)
        silver.write.mode("overwrite").partitionBy("month").parquet(str(silver_root))
    finally:
        silver.unpersist()

    written = spark.read.parquet(str(silver_root / f"month={month}")).count()
    if written != expected:
        raise DataQualityError(
            f"read back {written} rows from silver, expected {expected}"
        )

    return {
        "month": month,
        "bronze_rows": bronze_rows,
        "dropped_null_id": null_ids,
        "dropped_duplicates": bronze_rows - null_ids - expected,
        "silver_rows": written,
        "files": files,
        "runtime_seconds": round(time.monotonic() - started, 2),
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m lichess.spark_jobs.silver_games")
    ap.add_argument("--month", required=True, help="YYYY-MM")
    ap.add_argument("--bronze", type=Path, default=Path("data/bronze"))
    ap.add_argument("--silver", type=Path, default=Path("data/silver/games"))
    args = ap.parse_args(argv)

    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        stats = run(spark, args.month, args.bronze, args.silver)
    except DataQualityError as e:
        print(f"data quality check failed: {e}", file=sys.stderr)
        return EXIT_DATA_ERROR
    finally:
        spark.stop()
    print(json.dumps(stats, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
