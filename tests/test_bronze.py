import hashlib
import io
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import zstandard

from lichess import bronze
from lichess.bronze import BronzeError, write_partition

FIXTURES = Path(__file__).parent / "fixtures"
MONTH = "2013-02"


@pytest.fixture(scope="module")
def compressed() -> bytes:
    raw = (FIXTURES / "2013-02_first_500.pgn").read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def write(
    data: bytes, out: Path, expected: str | None = None, **kw
) -> bronze.BronzeStats:
    return write_partition(MONTH, io.BytesIO(data), out, expected, source="test", **kw)


def test_writes_partition_with_schema_and_stats(
    compressed: bytes, tmp_path: Path
) -> None:
    sha = hashlib.sha256(compressed).hexdigest()
    stats = write(compressed, tmp_path, expected=sha)

    part = tmp_path / f"month={MONTH}"
    table = pq.read_table(part / "part-00000.parquet")
    assert table.num_rows == 500
    assert table.schema.equals(bronze.SCHEMA)
    assert table.column("game_id").to_pylist()[0] == "9tp6v4ps"
    first = table.slice(0, 1).to_pylist()[0]
    assert str(first["utc_datetime"]) == "2013-01-31 23:01:50+00:00", first[
        "utc_datetime"
    ]
    assert stats.games == 500 and stats.malformed == 0 and stats.files == 1
    assert stats.bytes_read == len(compressed)
    assert stats.sha256 == sha and stats.checksum_verified

    on_disk = json.loads((part / "_stats.json").read_text())
    assert on_disk["games"] == 500 and on_disk["sha256"] == sha
    assert sorted(p.name for p in tmp_path.iterdir()) == [f"month={MONTH}"]


def test_rolls_files_and_row_groups(
    compressed: bytes, tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(bronze, "ROWS_PER_FILE", 200)
    monkeypatch.setattr(bronze, "ROWS_PER_BATCH", 50)
    stats = write(compressed, tmp_path)

    files = sorted((tmp_path / f"month={MONTH}").glob("part-*.parquet"))
    assert [p.name for p in files] == [f"part-0000{i}.parquet" for i in range(3)]
    assert [pq.ParquetFile(p).metadata.num_rows for p in files] == [200, 200, 100]
    assert pq.ParquetFile(files[0]).metadata.num_row_groups == 4
    assert stats.files == 3


def test_rerun_is_byte_identical(compressed: bytes, tmp_path: Path) -> None:
    write(compressed, tmp_path)
    first = (tmp_path / f"month={MONTH}" / "part-00000.parquet").read_bytes()
    write(compressed, tmp_path)
    second = (tmp_path / f"month={MONTH}" / "part-00000.parquet").read_bytes()
    assert first == second


def _existing_partition(root: Path) -> Path:
    part = root / f"month={MONTH}"
    part.mkdir(parents=True)
    (part / "marker").write_text("old")
    return part


def test_checksum_mismatch_leaves_old_partition(
    compressed: bytes, tmp_path: Path
) -> None:
    part = _existing_partition(tmp_path)
    with pytest.raises(BronzeError, match="sha256 mismatch"):
        write(compressed, tmp_path, expected="0" * 64)
    assert [p.name for p in part.iterdir()] == ["marker"]
    assert sorted(p.name for p in tmp_path.iterdir()) == [f"month={MONTH}"]


def test_truncated_stream_fails_and_leaves_old_partition(
    compressed: bytes, tmp_path: Path
) -> None:
    # Cutting a dropped connection's worth of bytes off the end: with no checksum to
    # compare, the truncated last game alone (1 of 500) must trip the malformed rate.
    part = _existing_partition(tmp_path)
    with pytest.raises((BronzeError, zstandard.ZstdError)):
        write(compressed[: len(compressed) - 200], tmp_path, max_malformed_rate=0.001)
    assert [p.name for p in part.iterdir()] == ["marker"]
    assert sorted(p.name for p in tmp_path.iterdir()) == [f"month={MONTH}"]


def test_successful_write_replaces_old_partition(
    compressed: bytes, tmp_path: Path
) -> None:
    part = _existing_partition(tmp_path)
    write(compressed, tmp_path)
    assert sorted(p.name for p in part.iterdir()) == [
        "_stats.json",
        "part-00000.parquet",
    ]
    assert sorted(p.name for p in tmp_path.iterdir()) == [f"month={MONTH}"]


def test_empty_input_fails(tmp_path: Path) -> None:
    with pytest.raises(BronzeError, match="no valid games"):
        write(zstandard.ZstdCompressor().compress(b""), tmp_path)
    assert not (tmp_path / f"month={MONTH}").exists()


def test_hashing_reader_drains_to_full_hash() -> None:
    data = b"x" * 3_000_000
    reader = bronze.HashingReader(io.BytesIO(data))
    reader.read(10)
    reader.drain()
    assert reader.bytes_read == len(data)
    assert reader.sha256.hexdigest() == hashlib.sha256(data).hexdigest()


@pytest.mark.parametrize("month", ["2013-2", "2013-13", "13-02", "2013-02x"])
def test_rejects_bad_month(month: str, tmp_path: Path) -> None:
    with pytest.raises(BronzeError, match="YYYY-MM"):
        bronze.run(month, "url", tmp_path)


def test_local_path_must_match_month_when_verifying(tmp_path: Path) -> None:
    with pytest.raises(BronzeError, match="--no-verify"):
        bronze.run(MONTH, str(FIXTURES / "2013-02_first_500.pgn"), tmp_path)


def test_is_current_needs_verified_matching_sha(
    compressed: bytes, tmp_path: Path
) -> None:
    sha = hashlib.sha256(compressed).hexdigest()
    assert not bronze.is_current(MONTH, tmp_path, sha)
    write(compressed, tmp_path)  # unverified: hash recorded, checksum_verified false
    assert not bronze.is_current(MONTH, tmp_path, sha)
    write(compressed, tmp_path, expected=sha)
    assert bronze.is_current(MONTH, tmp_path, sha)
    assert not bronze.is_current(MONTH, tmp_path, "0" * 64)


def test_skip_if_current_does_not_touch_partition(
    compressed: bytes, tmp_path: Path, monkeypatch, capsys
) -> None:
    sha = hashlib.sha256(compressed).hexdigest()
    write(compressed, tmp_path, expected=sha)
    part = tmp_path / f"month={MONTH}" / "part-00000.parquet"
    mtime = part.stat().st_mtime_ns
    monkeypatch.setattr(bronze, "published_sha256", lambda _name: sha)
    monkeypatch.setattr(
        bronze, "run", lambda *a, **k: pytest.fail("run() called on a current month")
    )

    code = bronze.main(["--month", MONTH, "--out", str(tmp_path), "--skip-if-current"])

    assert code == 0
    assert json.loads(capsys.readouterr().out) == {
        "status": "skipped",
        "month": MONTH,
        "sha256": sha,
    }
    assert part.stat().st_mtime_ns == mtime


def test_data_errors_exit_3_and_others_exit_1(tmp_path: Path, monkeypatch) -> None:
    def fail(exc: Exception):
        def _run(*a, **k):
            raise exc

        return _run

    monkeypatch.setattr(bronze, "run", fail(bronze.DataError("malformed rate")))
    assert bronze.main(["--month", MONTH, "--out", str(tmp_path)]) == 3
    monkeypatch.setattr(bronze, "run", fail(BronzeError("sha256 mismatch")))
    assert bronze.main(["--month", MONTH, "--out", str(tmp_path)]) == 1


def test_malformed_rate_is_a_data_error(compressed: bytes, tmp_path: Path) -> None:
    with pytest.raises(bronze.DataError):
        write(compressed[: len(compressed) - 200], tmp_path, max_malformed_rate=0.0)
