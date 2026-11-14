import io
import os
import shutil
import subprocess
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import zstandard

from lichess import bronze

FIXTURES = Path(__file__).parent / "fixtures"


def _java_works() -> bool:
    java = (
        Path(os.environ["JAVA_HOME"], "bin", "java")
        if "JAVA_HOME" in os.environ
        else None
    )
    exe = str(java) if java else shutil.which("java")
    # macOS ships a /usr/bin/java stub that exists but fails without a JDK installed.
    return (
        bool(exe)
        and subprocess.run(
            [exe, "-version"], capture_output=True, check=False
        ).returncode
        == 0
    )


pytestmark = pytest.mark.skipif(
    not _java_works(), reason="no Java runtime; set JAVA_HOME"
)


@pytest.fixture(scope="module")
def spark():
    from lichess.spark_jobs.silver_games import spark_session

    s = spark_session("test_silver", master="local[1]")
    s.conf.set("spark.sql.shuffle.partitions", "2")
    yield s
    s.stop()


def bronze_row(**overrides) -> dict:
    row = {
        "game_id": "abcdefgh",
        "event": "Rated Blitz game",
        "site": "https://lichess.org/abcdefgh",
        "utc_date": date(2013, 2, 1),
        "utc_datetime": datetime(2013, 2, 1, 12, 0, 0, tzinfo=UTC),
        "white": "a",
        "black": "b",
        "result": "1-0",
        "white_elo": 1500,
        "black_elo": 1500,
        "white_rating_diff": None,
        "black_rating_diff": None,
        "eco": "B20",
        "opening": "Sicilian Defense",
        "time_control": "300+0",
        "base_seconds": 300,
        "increment_seconds": 0,
        "termination": "Normal",
        "movetext": "1. e4 c5 2. Nf3 d6 1-0",
        "ply_count": 4,
        "has_clock": False,
        "has_eval": False,
    }
    row.update(overrides)
    return row


def write_bronze(root: Path, month: str, rows: list[dict]) -> None:
    part = root / f"month={month}"
    part.mkdir(parents=True)
    table = pa.Table.from_pylist(rows, schema=bronze.SCHEMA)
    pq.write_table(table, part / "part-00000.parquet")


def silver_rows(spark, rows: list[dict], month: str = "2013-02") -> list[dict]:
    from lichess.spark_jobs.silver_games import build_silver

    with tempfile.TemporaryDirectory() as d:
        write_bronze(Path(d), month, rows)
        df = build_silver(spark.read.parquet(f"{d}/month={month}"), month)
        return [r.asDict() for r in df.orderBy("game_id").collect()]


def ids(n: int) -> list[str]:
    return [f"g{i:07d}" for i in range(n)]


@pytest.mark.parametrize(
    ("tc", "base", "inc", "bucket"),
    [
        ("0+0", 0, 0, "ultrabullet"),
        ("28+0", 28, 0, "ultrabullet"),
        ("29+0", 29, 0, "bullet"),
        ("60+0", 60, 0, "bullet"),
        ("120+1", 120, 1, "bullet"),  # 160
        ("178+0", 178, 0, "bullet"),
        ("179+0", 179, 0, "blitz"),
        ("180+0", 180, 0, "blitz"),
        ("180+2", 180, 2, "blitz"),  # 260
        ("478+0", 478, 0, "blitz"),
        ("479+0", 479, 0, "rapid"),
        ("600+0", 600, 0, "rapid"),
        ("900+15", 900, 15, "classical"),  # 1500
        ("1498+0", 1498, 0, "rapid"),
        ("1499+0", 1499, 0, "classical"),
        ("-", None, None, "correspondence"),
    ],
)
def test_tc_bucket_boundaries(spark, tc, base, inc, bucket) -> None:
    row = bronze_row(time_control=tc, base_seconds=base, increment_seconds=inc)
    assert silver_rows(spark, [row])[0]["tc_bucket"] == bucket


def test_derived_columns(spark) -> None:
    rows = [
        bronze_row(game_id=i, **kw)
        for i, kw in zip(
            ids(7),
            [
                {
                    "white_elo": 1250,
                    "black_elo": 1349,
                    "opening": "Sicilian Defense: Najdorf",
                },
                {"white_elo": None, "opening": "Philidor Defense #3", "result": "0-1"},
                {
                    "opening": "French Defense: Advance Variation #3",
                    "result": "1/2-1/2",
                },
                {"opening": None, "eco": None, "result": "*"},
                {"termination": "Abandoned", "ply_count": 10},
                {"ply_count": 3},
                {"opening": "King's Gambit Declined, Queen's Knight Defense"},
            ],
            strict=True,
        )
    ]
    out = silver_rows(spark, rows)
    assert [r["rating_band"] for r in out] == [1200, None, 1500, 1500, 1500, 1500, 1500]
    assert [r["opening_family"] for r in out] == [
        "Sicilian Defense",
        "Philidor Defense",
        "French Defense",
        None,
        "Sicilian Defense",
        "Sicilian Defense",
        # Fragile: comma-style names without a colon stay their own family.
        "King's Gambit Declined, Queen's Knight Defense",
    ]
    assert [r["white_score"] for r in out] == [1.0, 0.0, 0.5, None, 1.0, 1.0, 1.0]
    assert [r["is_decisive_play"] for r in out] == [
        True,
        True,
        True,
        True,
        False,
        False,
        True,
    ]
    assert {r["month"] for r in out} == {"2013-02"}


def test_dedup_keeps_earliest(spark) -> None:
    early = datetime(2013, 2, 1, 10, tzinfo=UTC)
    late = datetime(2013, 2, 1, 11, tzinfo=UTC)
    rows = [
        bronze_row(game_id="dupdupdu", utc_datetime=late, white="late"),
        bronze_row(game_id="dupdupdu", utc_datetime=early, white="early"),
        bronze_row(game_id="uniqueid"),
    ]
    out = silver_rows(spark, rows)
    assert [(r["game_id"], r["white"]) for r in out] == [
        ("dupdupdu", "early"),
        ("uniqueid", "a"),
    ]


def test_run_end_to_end_and_rerun(spark, tmp_path: Path) -> None:
    from lichess.spark_jobs.silver_games import run

    raw = zstandard.ZstdCompressor().compress(
        (FIXTURES / "2013-02_first_500.pgn").read_bytes()
    )
    bronze.write_partition(
        "2013-02", io.BytesIO(raw), tmp_path / "bronze", None, source="t"
    )
    write_bronze(tmp_path / "bronze", "2013-03", [bronze_row()])
    silver_root = tmp_path / "silver"

    run(spark, "2013-03", tmp_path / "bronze", silver_root)
    first = run(spark, "2013-02", tmp_path / "bronze", silver_root)
    second = run(spark, "2013-02", tmp_path / "bronze", silver_root)

    assert first["silver_rows"] == second["silver_rows"] == 500
    assert first["dropped_duplicates"] == 0
    # Dynamic overwrite: rewriting 2013-02 twice didn't touch 2013-03.
    counts = {
        r["month"]: r["count"]
        for r in spark.read.parquet(str(silver_root)).groupBy("month").count().collect()
    }
    assert counts == {"2013-02": 500, "2013-03": 1}


def test_run_counts_dropped_duplicates(spark, tmp_path: Path) -> None:
    from lichess.spark_jobs.silver_games import run

    write_bronze(
        tmp_path / "b",
        "2013-02",
        [bronze_row(), bronze_row(), bronze_row(game_id="x" * 8)],
    )
    stats = run(spark, "2013-02", tmp_path / "b", tmp_path / "s")
    assert (
        stats["bronze_rows"],
        stats["dropped_duplicates"],
        stats["silver_rows"],
    ) == (3, 1, 2)


def test_quality_check_fails_on_null_tc_bucket(spark, tmp_path: Path) -> None:
    from lichess.spark_jobs.silver_games import DataQualityError, run

    bad = bronze_row(time_control="300+0", base_seconds=None, increment_seconds=None)
    write_bronze(tmp_path / "b", "2013-02", [bad])
    with pytest.raises(DataQualityError, match="null tc_bucket"):
        run(spark, "2013-02", tmp_path / "b", tmp_path / "s")
    assert not (tmp_path / "s").exists()


def test_quality_check_fails_on_duplicate_ids(spark) -> None:
    from lichess.spark_jobs.silver_games import (
        DataQualityError,
        add_columns,
        check_quality,
    )

    with tempfile.TemporaryDirectory() as d:
        write_bronze(Path(d), "2013-02", [bronze_row(), bronze_row()])
        # Skip dedup on purpose to prove the check itself catches duplicates.
        df = add_columns(spark.read.parquet(f"{d}/month=2013-02"), "2013-02")
        with pytest.raises(DataQualityError, match="not unique"):
            check_quality(df, expected_rows=2)
