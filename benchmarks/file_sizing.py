"""Many tiny Parquet files vs. a few large ones: write time, read time, file count.

    JAVA_HOME=/opt/homebrew/opt/openjdk@17 uv run python benchmarks/file_sizing.py \
        --source data/bronze/month=2013-0[1-6] --files 1000 100 10 1 \
        --out <scratch dir>

The same rows are written once per layout with Spark (`repartition(n)`, so every
layout pays the same shuffle and gets exactly n files), then read back by Spark and
DuckDB with two queries: a narrow group-by on two small columns and a full scan that
touches the widest column. Each read builds a new DataFrame / `read_parquet` call, so
file listing and footer reads are inside the timing. The OS page cache is warm for
every layout alike; this measures per-file overhead, not disk throughput.
"""

import argparse
import json
import math
import shutil
import statistics
import time
from pathlib import Path

NARROW = "select eco, count(*) as n, sum(ply_count) as plies from t group by eco"
WIDE_COL = "movetext"
# Overridden to a silver column when the source has no movetext (see main()).
WIDE = "select count(game_id) as n, sum(length({col})) as chars, sum(ply_count) as plies from t"


def best(fn, repeat: int) -> tuple[float, float, object]:
    times, out = [], None
    for _ in range(repeat):
        t = time.perf_counter()
        out = fn()
        times.append(time.perf_counter() - t)
    return times[0], min(times[1:] or times), out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", nargs="+", required=True, help="Parquet dirs")
    ap.add_argument("--files", type=int, nargs="+", default=[1000, 100, 10, 1])
    ap.add_argument("--out", required=True, help="scratch dir, deleted at the end")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--keep", action="store_true")
    ap.add_argument(
        "--method",
        choices=["repartition", "split"],
        default="repartition",
        help="repartition: round-robin shuffle into exactly n files. split: no "
        "shuffle; coalesce to at most n tasks and cap rows per file at rows/n "
        "(file count is then about n)",
    )
    ap.add_argument(
        "--no-cache",
        action="store_true",
        help="re-read the source for each layout instead of caching it (for inputs "
        "bigger than the driver heap; write times then include the source read)",
    )
    args = ap.parse_args()

    import duckdb
    import pyspark

    from lichess.spark_jobs.silver_games import spark_session

    out = Path(args.out)
    spark = spark_session("file_sizing")
    spark.sparkContext.setLogLevel("ERROR")
    src = spark.read.parquet(*args.source)
    col = WIDE_COL if WIDE_COL in src.columns else "opening"
    wide = WIDE.format(col=col)
    if not args.no_cache:
        src = src.cache()
    rows = src.count()
    src_mb = (
        sum(p.stat().st_size for d in args.source for p in Path(d).glob("*.parquet"))
        / 1e6
    )
    print(
        json.dumps(
            {
                "source": args.source,
                "rows": rows,
                "source_mb": round(src_mb, 1),
                "wide_column": col,
                "cached": not args.no_cache,
                "method": args.method,
                "spark": pyspark.__version__,
                "cores": spark.sparkContext.defaultParallelism,
                "max_partition_bytes": spark.conf.get(
                    "spark.sql.files.maxPartitionBytes"
                ),
                "open_cost_bytes": spark.conf.get("spark.sql.files.openCostInBytes"),
                "duckdb": duckdb.__version__,
            }
        ),
        flush=True,
    )

    results = []
    try:
        for n in args.files:
            path = out / f"files={n}"
            t = time.perf_counter()
            if args.method == "repartition":
                src.repartition(n).write.mode("overwrite").parquet(str(path))
            else:
                # Round-robin repartition sorts every row first; on 88M full-width
                # silver rows that ran a 4 GB driver out of heap. This never shuffles.
                parts_in = src.rdd.getNumPartitions()
                src.coalesce(min(n, parts_in)).write.option(
                    "maxRecordsPerFile", math.ceil(rows / n)
                ).mode("overwrite").parquet(str(path))
            write_s = time.perf_counter() - t
            parts = sorted(path.glob("*.parquet"))
            sizes = [p.stat().st_size for p in parts]

            def spark_q(sql: str, p: str = str(path)):
                spark.read.parquet(p).createOrReplaceTempView("t")
                return sorted((tuple(r) for r in spark.sql(sql).collect()), key=str)

            s_narrow = best(lambda: spark_q(NARROW), args.repeat)
            s_wide = best(lambda: spark_q(wide), args.repeat)
            in_parts = spark.read.parquet(str(path)).rdd.getNumPartitions()

            glob = f"{path}/*.parquet"

            def duck_q(sql: str, glob: str = glob):
                con = duckdb.connect()
                con.execute(f"create view t as select * from read_parquet('{glob}')")
                r = sorted(con.execute(sql).fetchall(), key=str)  # eco can be null
                con.close()
                return r

            d_narrow = best(lambda: duck_q(NARROW), args.repeat)
            d_wide = best(lambda: duck_q(wide), args.repeat)

            r = {
                "files": len(parts),
                "total_mb": round(sum(sizes) / 1e6, 1),
                "median_file_mb": round(statistics.median(sizes) / 1e6, 3),
                "spark_write_s": round(write_s, 2),
                "spark_read_tasks": in_parts,
                "spark_narrow_first_s": round(s_narrow[0], 3),
                "spark_narrow_best_s": round(s_narrow[1], 3),
                "spark_wide_first_s": round(s_wide[0], 3),
                "spark_wide_best_s": round(s_wide[1], 3),
                "duckdb_narrow_first_s": round(d_narrow[0], 3),
                "duckdb_narrow_best_s": round(d_narrow[1], 3),
                "duckdb_wide_first_s": round(d_wide[0], 3),
                "duckdb_wide_best_s": round(d_wide[1], 3),
                # Same answers in every layout and both engines, or the timings are moot.
                "wide_result": [list(x) for x in s_wide[2]],
                "results_match": s_narrow[2] == [tuple(x) for x in d_narrow[2]]
                and s_wide[2] == [tuple(x) for x in d_wide[2]],
            }
            results.append(r)
            print(json.dumps(r), flush=True)
            if not args.keep:
                shutil.rmtree(path)
    finally:
        spark.stop()
        if not args.keep:
            shutil.rmtree(out, ignore_errors=True)

    print(
        "| Files | Total MB | Median file MB | Spark write s | Spark read tasks "
        "| Spark narrow s | Spark full s | DuckDB narrow s | DuckDB full s |"
    )
    print("|---|---|---|---|---|---|---|---|---|")
    for r in results:
        print(
            f"| {r['files']:,} | {r['total_mb']} | {r['median_file_mb']} "
            f"| {r['spark_write_s']} | {r['spark_read_tasks']} "
            f"| {r['spark_narrow_best_s']} | {r['spark_wide_best_s']} "
            f"| {r['duckdb_narrow_best_s']} | {r['duckdb_wide_best_s']} |"
        )


if __name__ == "__main__":
    main()
