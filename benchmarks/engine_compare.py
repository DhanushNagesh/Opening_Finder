"""Gold aggregation (fct_opening_performance) in DuckDB vs. Spark local[*], same silver.

    uv run python benchmarks/engine_compare.py --engine duckdb --months 2013-02
    JAVA_HOME=/opt/homebrew/opt/openjdk@17 uv run python benchmarks/engine_compare.py --engine spark --months 2013-01 2013-02 2013-03 2013-04 2013-05 2013-06

The SQL is `stg_games`' filter plus `fct_opening_performance` with the Wilson macro
inlined, written once in syntax both engines accept (explicit GROUP BY, FILTER). Both
engines materialise the full result without shipping it to Python: DuckDB into a temp
table, Spark to the `noop` sink. A fingerprint of the result is printed so the two
runs can be checked for identical output. Run one engine per process.
"""

import argparse
import json
import platform
import time
from pathlib import Path

SILVER = Path("data/silver/games")
Z = 1.96

QUERY = f"""
with stg as (
    select * from games
    where is_decisive_play and rating_band is not null and white_score is not null
),
by_side as (
    select opening_family, opening, eco, rating_band, tc_bucket,
           'white' as color, white_score as score, ply_count
    from stg
    union all
    select opening_family, opening, eco, rating_band, tc_bucket,
           'black' as color, 1 - white_score as score, ply_count
    from stg
),
agg as (
    select opening_family, opening, eco, rating_band, tc_bucket, color,
           count(*) as games,
           count(*) filter (where score = 1) as wins,
           count(*) filter (where score = 0.5) as draws,
           count(*) filter (where score = 0) as losses,
           avg(score) as score,
           avg(ply_count) as avg_ply
    from by_side
    group by opening_family, opening, eco, rating_band, tc_bucket, color
)
select *,
    greatest(0.0, (score + {Z * Z} / (2.0 * games)
        - {Z} * sqrt(score * (1 - score) / games + {Z * Z} / (4.0 * games * games))
    ) / (1 + {Z * Z} / games)) as score_wilson_lb
from agg
"""

FINGERPRINT = """
select count(*) as cells, sum(games) as games, sum(wins) as wins,
       sum(draws) as draws, round(sum(score_wilson_lb), 6) as wilson_sum
from result
"""


def files_for(months: list[str]) -> list[str]:
    out = []
    for m in months:
        parts = sorted((SILVER / f"month={m}").glob("*.parquet"))
        if not parts:
            raise SystemExit(f"no silver Parquet for {m}")
        out += [str(p) for p in parts]
    return out


def run_duckdb(files: list[str], repeat: int) -> dict:
    import duckdb

    t0 = time.perf_counter()
    con = duckdb.connect()
    # Views can't take prepared parameters; the paths are our own silver files.
    paths = ", ".join(f"'{f}'" for f in files)
    con.execute(
        f"create view games as select * from read_parquet([{paths}], "
        "hive_partitioning=false)"
    )
    startup = time.perf_counter() - t0
    times = []
    for _ in range(repeat):
        t = time.perf_counter()
        con.execute(f"create or replace temp table result as {QUERY}")
        times.append(time.perf_counter() - t)
    fp = con.execute(FINGERPRINT).fetchone()
    rows = con.execute("select count(*) from games").fetchone()[0]
    threads = con.execute("select current_setting('threads')").fetchone()[0]
    return {
        "engine": f"duckdb {duckdb.__version__} threads={threads}",
        "startup_s": startup,
        "times": times,
        "fingerprint": list(fp),
        "silver_rows": rows,
    }


def run_spark(files: list[str], repeat: int) -> dict:
    import pyspark

    from lichess.spark_jobs.silver_games import spark_session

    t0 = time.perf_counter()
    spark = spark_session("engine_compare")
    spark.sparkContext.setLogLevel("ERROR")
    startup = time.perf_counter() - t0
    try:
        spark.read.parquet(*files).createOrReplaceTempView("games")
        times = []
        for _ in range(repeat):
            t = time.perf_counter()
            spark.sql(QUERY).write.format("noop").mode("overwrite").save()
            times.append(time.perf_counter() - t)
        spark.sql(QUERY).createOrReplaceTempView("result")
        fp = spark.sql(FINGERPRINT).first()
        rows = spark.table("games").count()
        shuffle = spark.conf.get("spark.sql.shuffle.partitions")
        aqe = spark.conf.get("spark.sql.adaptive.enabled")
        cores = spark.sparkContext.defaultParallelism
    finally:
        spark.stop()
    return {
        "engine": f"spark {pyspark.__version__} local[*] cores={cores} "
        f"shuffle.partitions={shuffle} aqe={aqe} driver=4g",
        "startup_s": startup,
        "times": times,
        "fingerprint": [fp[0], fp[1], fp[2], fp[3], float(fp[4])],
        "silver_rows": rows,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", choices=["duckdb", "spark"], required=True)
    ap.add_argument("--months", nargs="+", required=True)
    ap.add_argument("--repeat", type=int, default=3)
    args = ap.parse_args()

    files = files_for(args.months)
    mb = sum(Path(f).stat().st_size for f in files) / 1e6
    run = run_duckdb if args.engine == "duckdb" else run_spark
    r = run(files, args.repeat)
    print(
        json.dumps(
            {
                "months": args.months,
                "silver_mb": round(mb, 1),
                "silver_rows": r["silver_rows"],
                "engine": r["engine"],
                "startup_s": round(r["startup_s"], 2),
                "first_s": round(r["times"][0], 3),
                "best_warm_s": round(min(r["times"][1:] or r["times"]), 3),
                "fingerprint": r["fingerprint"],
                "python": platform.python_version(),
            }
        )
    )


if __name__ == "__main__":
    main()
