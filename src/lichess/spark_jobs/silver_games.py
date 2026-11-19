"""Bronze month -> silver games, with data-quality checks that fail the job.

    JAVA_HOME=/opt/homebrew/opt/openjdk@17 uv run python -m lichess.spark_jobs.silver_games --month 2013-02

Writes the month to a staging dir, runs the checks on the written files, then swaps
it into `silver_root/month=YYYY-MM/`, so only that month is replaced and a failed check
leaves the old partition in place (D-005, D-025). Exit code 3 means a data-quality failure, which a retry
won't fix; any other non-zero exit (JVM crash, OOM) may be worth retrying.
"""

import argparse
import json
import math
import os
import shutil
import sys
import time
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lichess.bronze import swap_in

# Target rows per silver file. coalesce can only merge input splits, so a file is at
# most this big and at least one bronze split (D-017, D-025).
ROWS_PER_FILE = 500_000
EXIT_DATA_ERROR = 3
# Mate scores have no centipawn value. Store them as +/- this sentinel (sign = side that
# mates); gold clamps every eval well inside it before measuring swings (D-022).
MATE_CP = 10_000
# One match per SAN move plus the comment right after it, if any. SAN starts with a
# letter; move numbers and result tokens start with a digit or `*`, so they never match.
_MOVE_WITH_COMMENT = r"([A-Za-z][^\s{}()]*)(?:\s*\{([^}]*)\})?"


class DataQualityError(RuntimeError):
    pass


def spark_session(
    app_name: str = "silver_games", master: str = "local[*]"
) -> SparkSession:
    return (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
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


def evals_cp(movetext: Column, has_eval: Column) -> Column:
    """Eval after each ply in centipawns, one element per move; null where a move has no
    eval. The whole column is null for games without any `[%eval]` (D-022)."""
    comments = F.regexp_extract_all(movetext, F.lit(_MOVE_WITH_COMMENT), 2)

    def to_cp(comment: Column) -> Column:
        raw = F.regexp_extract(comment, r"\[%eval\s+(#?-?[0-9.]+)", 1)
        return (
            F.when(raw == "", F.lit(None).cast("int"))
            .when(raw.startswith("#-"), -MATE_CP)
            .when(raw.startswith("#"), MATE_CP)
            .otherwise(F.round(raw.cast("double") * 100).cast("int"))
        )

    return F.when(has_eval, F.transform(comments, to_cp))


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
        evals_cp(F.col("movetext"), F.col("has_eval")).alias("evals_cp"),
        F.lit(month).alias("month"),
    )


def dedup(df: DataFrame) -> DataFrame:
    """Drop null ids and keep one row per game_id, shuffling only the id column.

    A window over all rows would shuffle every column, movetext included: 5.5x the
    compressed input in Spark temp files on a 2024 sample (D-025). Instead, find the
    duplicated ids from the id column alone, pass unique rows through untouched, and run
    the window only on the duplicated rows.
    """
    df = df.where(F.col("game_id").isNotNull())
    # Broadcast: duplicates are rare (none in 2013 H1), so this list is tiny.
    dup_ids = F.broadcast(
        df.groupBy("game_id").count().where("count > 1").select("game_id")
    )
    unique = df.join(dup_ids, "game_id", "left_anti")
    # Deterministic winner so a rerun keeps the same row when duplicates differ.
    w = Window.partitionBy("game_id").orderBy("utc_datetime", "movetext")
    winners = (
        df.join(dup_ids, "game_id", "left_semi")
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn")
    )
    return unique.unionByName(winners)


def build_silver(bronze: DataFrame, month: str) -> DataFrame:
    return add_columns(dedup(bronze), month)


def check_quality(silver: DataFrame, expected_rows: int) -> dict[str, int]:
    row = silver.agg(
        F.count("*").alias("rows"),
        F.countDistinct("game_id").alias("distinct_ids"),
        F.sum(
            (F.col("tc_bucket").isNull() & (F.col("time_control") != "-")).cast("int")
        ).alias("null_tc_bucket"),
        # The eval array is cut by a different regex than the parser's ply count, so a
        # mismatch means one of them misreads some movetext and the evals are misaligned.
        F.sum(
            (F.col("has_eval") & (F.size("evals_cp") != F.col("ply_count"))).cast("int")
        ).alias("eval_misaligned"),
    ).first()
    rows, distinct_ids, null_tc, misaligned = (
        row["rows"],
        row["distinct_ids"],
        row["null_tc_bucket"] or 0,
        row["eval_misaligned"] or 0,
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
    if misaligned:
        failures.append(f"{misaligned} games where evals_cp length != ply_count")
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

    # Staging sits next to silver_root, not inside it, so readers globbing silver never
    # see it; same filesystem, so the swap is two renames.
    tmp = (
        silver_root.parent
        / f"_staging_{silver_root.name}"
        / f"month={month}-{os.getpid()}"
    )
    shutil.rmtree(tmp, ignore_errors=True)
    files = max(1, math.ceil(expected / ROWS_PER_FILE))
    try:
        # coalesce merges input splits without a shuffle, so file size costs no temp
        # disk. Checks run on what was written rather than on a cached frame.
        build_silver(bronze, month).drop("month").coalesce(files).write.parquet(
            str(tmp)
        )
        quality = check_quality(spark.read.parquet(str(tmp)), expected)
        files = len(list(tmp.glob("*.parquet")))
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    silver_root.mkdir(parents=True, exist_ok=True)
    swap_in(tmp, silver_root / f"month={month}")
    try:
        tmp.parent.rmdir()
    except OSError:  # another month's staging dir is still there
        pass
    written = quality["rows"]

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
