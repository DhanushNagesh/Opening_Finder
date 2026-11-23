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


class _FlakyConn:
    """One HTTP body starting at `start`; fails after `fail_after` bytes, either by
    raising (a stall or reset) or by ending early (a server closing cleanly)."""

    def __init__(self, data: bytes, start: int, fail_after: int | None, mode: str):
        self._buf = io.BytesIO(data[start:])
        self._left = fail_after
        self._mode = mode

    def read(self, size: int = -1) -> bytes:
        if self._left is not None and self._left <= 0:
            if self._mode == "raise":
                raise TimeoutError("The read operation timed out")
            return b""
        n = size if size >= 0 else 1 << 30
        if self._left is not None:
            n = min(n, self._left)
            self._left -= n
        return self._buf.read(n)

    def close(self) -> None:
        pass


def _flaky_reader(data: bytes, drops: int, every: int, mode: str, **kw):
    opens: list[int] = []

    def open_at(start: int) -> _FlakyConn:
        opens.append(start)
        fail = every if len(opens) <= drops else None
        return _FlakyConn(data, start, fail, mode)

    reader = bronze.ResumingReader(open_at, len(data), sleep=lambda _s: None, **kw)
    return reader, opens


@pytest.mark.parametrize("mode", ["raise", "early_eof"])
def test_resuming_reader_gives_identical_partition(
    compressed: bytes, tmp_path: Path, mode: str
) -> None:
    expected = hashlib.sha256(compressed).hexdigest()
    clean = write(compressed, tmp_path / "clean", expected)

    every = len(compressed) // 7 + 1
    reader, opens = _flaky_reader(compressed, drops=5, every=every, mode=mode)
    flaky = write_partition(MONTH, reader, tmp_path / "flaky", expected, source="test")

    assert reader.resumes == 5
    assert opens == [i * every for i in range(6)]  # each resume starts at the next byte
    assert (flaky.sha256, flaky.games, flaky.bytes_read) == (
        clean.sha256,
        clean.games,
        len(compressed),
    )
    for a, b in zip(
        sorted((tmp_path / "clean" / f"month={MONTH}").glob("*.parquet")),
        sorted((tmp_path / "flaky" / f"month={MONTH}").glob("*.parquet")),
        strict=True,
    ):
        assert a.read_bytes() == b.read_bytes()


def test_resuming_reader_gives_up_and_leaves_old_partition(
    compressed: bytes, tmp_path: Path
) -> None:
    part = _existing_partition(tmp_path)
    # every=0: each connection fails before sending a byte, so no resume makes progress.
    reader, _ = _flaky_reader(
        compressed, drops=100, every=0, mode="raise", max_stalled=3
    )
    with pytest.raises(BronzeError, match="gave up after 3 resumes with no progress"):
        write_partition(MONTH, reader, tmp_path, None, source="test")
    assert [p.name for p in part.iterdir()] == ["marker"]


def test_drops_with_progress_never_hit_the_limit(compressed: bytes) -> None:
    # 2024-10 failed after 50 resumes spread over 22 h, each one after new bytes.
    every = len(compressed) // 60 + 1
    reader, _ = _flaky_reader(
        compressed, drops=59, every=every, mode="raise", max_stalled=3
    )
    out = b""
    while chunk := reader.read(4096):
        out += chunk
    assert out == compressed
    assert (reader.resumes, reader.stalled) == (59, 1)


def test_resume_without_length_fails() -> None:
    conn = _FlakyConn(b"x" * 10, 0, 4, "raise")
    reader = bronze.ResumingReader(lambda s: conn, 0, first=conn, sleep=lambda _s: None)
    assert reader.read(4) == b"xxxx"
    with pytest.raises(BronzeError, match="length is unknown"):
        reader.read(4)


def test_open_range_rejects_server_that_ignores_range(monkeypatch) -> None:
    class Resp:
        def __init__(self) -> None:
            self.status = 200
            self.headers = {"Content-Length": "10"}

        def close(self) -> None:
            pass

    monkeypatch.setattr(bronze.urllib.request, "urlopen", lambda req, timeout: Resp())
    with pytest.raises(BronzeError, match="resume at 5 got HTTP 200"):
        bronze.open_range("https://example.invalid/x", 5)
