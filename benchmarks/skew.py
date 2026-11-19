"""Skew on `opening_family`: a group-by, a window and a join, with per-task numbers
read from the Spark UI's REST API (the same data as the stage page's task table).

    JAVA_HOME=/opt/homebrew/opt/openjdk@17 uv run python benchmarks/skew.py \
        --months 2013-01 2013-02 2013-03 2013-04 2013-05 2013-06 --replicate 20

`--replicate k` repeats every silver row k times (each copy gets its own `rep`, so a
salt hashed from (game_id, rep) still spreads it). Every key's share of rows is
unchanged, so the skew is real; only the absolute size is scaled up so tasks are long
enough to time. The join is games -> a per-family lookup table (one row per family, built
as a local table so neither side is pre-partitioned), with broadcast off so Spark has to shuffle it, which is the case where skew bites.

Variants:
  groupby            count/avg per family; partial aggregation runs before the shuffle
  window             row_number() over (partition by family): no partial step exists
  join_baseline      sort-merge join, AQE on but its skew-join rule off
  join_aqe_skew      AQE skew join, with the threshold lowered to fit this data size
  join_aqe_skew_cached_lookup
                     same, but the lookup is a cached group-by output (pre-partitioned)
  join_salted        big side salted into N buckets, lookup replicated N times
  join_broadcast     the lookup broadcast instead (no shuffle of the big side at all)
"""

import argparse
import json
import statistics
import time
import urllib.request
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER = Path("data/silver/games")
SALT = 16


def rest(spark: SparkSession, path: str):
    sc = spark.sparkContext
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/{path}"
    with urllib.request.urlopen(url) as r:
        return json.load(r)


def stage_metrics(spark: SparkSession, group: str) -> dict:
    """Task durations and shuffle-read records for the heaviest shuffle-reading stage
    of a job group: the stage that holds the skewed key."""
    # The UI's listener bus is asynchronous; wait until the group's jobs show up done.
    for _ in range(100):
        jobs = [j for j in rest(spark, "jobs") if j.get("jobGroup") == group]
        if jobs and all(j["status"] != "RUNNING" for j in jobs):
            break
        time.sleep(0.1)
    best, stages = None, []
    for sid in sorted({s for j in jobs for s in j["stageIds"]}):
        for attempt in rest(spark, f"stages/{sid}"):
            if attempt["status"] != "COMPLETE":
                continue
            tasks = rest(
                spark,
                f"stages/{sid}/{attempt['attemptId']}/taskList?length=100000",
            )
            durs = [t["taskMetrics"]["executorRunTime"] for t in tasks]
            recs = [
                t["taskMetrics"]["shuffleReadMetrics"]["recordsRead"] for t in tasks
            ]
            stages.append(
                {
                    "stage": sid,
                    "tasks": len(tasks),
                    "wall_ms": _ms(attempt["completionTime"])
                    - _ms(attempt["submissionTime"]),
                    "task_ms_sum": sum(durs),
                    "max_task_ms": max(durs),
                }
            )
            if not sum(recs):
                continue
            mb = [
                t["taskMetrics"]["shuffleReadMetrics"]["localBytesRead"]
                + t["taskMetrics"]["shuffleReadMetrics"]["remoteBytesRead"]
                for t in tasks
            ]
            if best is None or sum(recs) > best["shuffle_records"]:
                med = statistics.median(durs)
                best = {
                    "stage": sid,
                    "tasks": len(tasks),
                    "shuffle_records": sum(recs),
                    "max_task_records": max(recs),
                    "max_task_shuffle_mb": round(max(mb) / 1e6, 1),
                    "median_task_shuffle_mb": round(statistics.median(mb) / 1e6, 3),
                    "median_task_records": statistics.median(recs),
                    "max_task_ms": max(durs),
                    "median_task_ms": med,
                    "max_over_median": round(max(durs) / med, 1) if med else None,
                    "task_ms_sum": sum(durs),
                }
    return {"skew_stage": best, "stages": stages}


def _ms(ts: str) -> int:
    # e.g. "2026-11-18T23:01:02.345GMT"
    return int(
        datetime.strptime(ts.replace("GMT", ""), "%Y-%m-%dT%H:%M:%S.%f").timestamp()
        * 1000
    )


def games(spark: SparkSession, months: list[str], k: int) -> DataFrame:
    df = spark.read.parquet(*[str(SILVER / f"month={m}") for m in months]).select(
        "game_id", "opening_family", "white_elo", "white_score"
    )
    df = df.where(F.col("opening_family").isNotNull())
    if k > 1:
        df = df.crossJoin(spark.range(k).withColumnRenamed("id", "rep"))
    else:
        df = df.withColumn("rep", F.lit(0))
    return df


def lookup(g: DataFrame) -> DataFrame:
    return g.groupBy("opening_family").agg(
        F.avg("white_elo").alias("family_elo"), F.count("*").alias("family_games")
    )


def run_variant(
    spark: SparkSession, name: str, g: DataFrame, lk: DataFrame, lk_cached: DataFrame
) -> DataFrame:
    conf = spark.conf
    conf.set("spark.sql.adaptive.enabled", "true")
    # Off everywhere so partition counts differ only by the fix being measured.
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    conf.unset("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes")
    conf.unset("spark.sql.adaptive.advisoryPartitionSizeInBytes")

    if name == "groupby":
        return g.groupBy("opening_family").agg(
            F.count("*").alias("n"), F.avg("white_score").alias("score")
        )
    if name == "window":
        w = Window.partitionBy("opening_family").orderBy("white_elo", "game_id", "rep")
        return g.withColumn("rank_in_family", F.row_number().over(w))
    if name == "join_baseline":
        return g.join(lk, "opening_family")
    if name == "join_aqe_skew":
        conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        # Default 256 MB: no partition at this size reaches it, so the rule wouldn't fire.
        conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "8MB")
        # A skewed partition is cut into pieces of this size (default 64 MB); left at
        # the default, the hot partition here would be "split" into one piece.
        conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "2MB")
        return g.join(lk, "opening_family")
    if name == "join_aqe_skew_cached_lookup":
        # Same rule and threshold, but the lookup is the cached output of a group-by on
        # opening_family: already hash-partitioned on the join key, so only the big
        # side gets a shuffle and the skew-join rule (which needs a shuffle stage on
        # both sides) never applies.
        conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "8MB")
        # A skewed partition is cut into pieces of this size (default 64 MB); left at
        # the default, the hot partition here would be "split" into one piece.
        conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "2MB")
        return g.join(lk_cached, "opening_family")
    if name == "join_salted":
        salted = g.withColumn(
            "salt", F.pmod(F.xxhash64("game_id", "rep"), F.lit(SALT)).cast("int")
        )
        lk_n = lk.crossJoin(
            spark.range(SALT).select(F.col("id").cast("int").alias("salt"))
        )
        return salted.join(lk_n, ["opening_family", "salt"]).drop("salt")
    if name == "join_broadcast":
        return g.join(F.broadcast(lk), "opening_family")
    raise ValueError(name)


VARIANTS = [
    "groupby",
    "window",
    "join_baseline",
    "join_aqe_skew",
    "join_aqe_skew_cached_lookup",
    "join_salted",
    "join_broadcast",
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", nargs="+", required=True)
    ap.add_argument("--replicate", type=int, default=1)
    ap.add_argument("--variants", nargs="+", default=VARIANTS, choices=VARIANTS)
    ap.add_argument("--repeat", type=int, default=2, help="best wall time is kept")
    args = ap.parse_args()

    import pyspark

    from lichess.spark_jobs.silver_games import spark_session

    spark = spark_session("skew")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        g = games(spark, args.months, args.replicate).cache()
        rows = g.count()
        lk_cached = lookup(g).cache()
        # The lookup table as it would arrive from a file or a seed: ~300 local rows
        # with no partitioning, so a join has to shuffle both sides.
        lk = spark.createDataFrame(lk_cached.collect(), lk_cached.schema)
        families = lk.count()
        top = lk.orderBy(F.desc("family_games")).first()
        print(
            json.dumps(
                {
                    "months": args.months,
                    "replicate": args.replicate,
                    "rows": rows,
                    "families": families,
                    "top_family": top["opening_family"],
                    "top_share": round(top["family_games"] / rows, 4),
                    "spark": pyspark.__version__,
                    "cores": spark.sparkContext.defaultParallelism,
                    "shuffle_partitions": spark.conf.get(
                        "spark.sql.shuffle.partitions"
                    ),
                    "salt_buckets": SALT,
                }
            ),
            flush=True,
        )
        for name in args.variants:
            best_s, metrics = float("inf"), {}
            for i in range(args.repeat):
                group = f"{name}-{i}"
                spark.sparkContext.setJobGroup(group, group)
                df = run_variant(spark, name, g, lk, lk_cached)
                t = time.perf_counter()
                df.write.format("noop").mode("overwrite").save()
                wall = time.perf_counter() - t
                if wall < best_s:
                    best_s, metrics = wall, stage_metrics(spark, group)
            print(
                json.dumps({"variant": name, "wall_s": round(best_s, 2), **metrics}),
                flush=True,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
