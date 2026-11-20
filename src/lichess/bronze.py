"""Raw Lichess month -> bronze Parquet.

    uv run python -m lichess.bronze --month 2013-02
    uv run python -m lichess.bronze --month 2013-02 --source data/raw/lichess_db_standard_rated_2013-02.pgn.zst
    uv run python -m lichess.bronze --month 2013-02 --skip-if-current

Exit codes: 0 written or skipped (the JSON on stdout says which), 1 retryable failure
(network, checksum), 3 data error that a retry won't fix (malformed rate, no games).

URL mode streams HTTP -> SHA-256 -> zstd -> parser -> Parquet; nothing raw touches disk
(D-009). The partition is built in a temp dir and swapped in only after the checksum
matches, so a failed run leaves the previous partition as it was (D-005).
"""

import argparse
import hashlib
import http.client
import json
import logging
import os
import re
import shutil
import sys
import time
import urllib.request
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO

import pyarrow as pa
import pyarrow.parquet as pq

from lichess.models import Game, ParseStats
from lichess.parser import PgnParser, text_stream

log = logging.getLogger(__name__)

BASE_URL = "https://database.lichess.org/standard"
CHECKSUM_URL = f"{BASE_URL}/sha256sums.txt"
USER_AGENT = "opening-finder-bronze/0.1"

ROWS_PER_FILE = 250_000
ROWS_PER_BATCH = 50_000  # one row group; bounds memory to one batch of Python objects
# A clean month has zero malformed games; one or two can come from Lichess-side oddities.
# More than this means format drift or a truncated stream, and the month fails (D-012, D-014).
MAX_MALFORMED_RATE = 0.001
# Bronze was 1.42x the compressed source for 2013-02 (25.7 MB vs 18.2 MB).
# Require 2x free so a big month can't fill the disk halfway through.
DISK_HEADROOM = 2.0

SCHEMA = pa.schema(
    [
        pa.field("game_id", pa.string(), nullable=False),
        pa.field("event", pa.string(), nullable=False),
        pa.field("site", pa.string(), nullable=False),
        pa.field("utc_date", pa.date32(), nullable=False),
        # Parquet has no seconds unit for TIME or TIMESTAMP, and Spark 3.5 can't read TIME,
        # so time of day is stored as a full UTC timestamp alongside the date (D-013).
        pa.field("utc_datetime", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("white", pa.string(), nullable=False),
        pa.field("black", pa.string(), nullable=False),
        pa.field("result", pa.string(), nullable=False),
        pa.field("white_elo", pa.int32()),
        pa.field("black_elo", pa.int32()),
        pa.field("white_rating_diff", pa.int32()),
        pa.field("black_rating_diff", pa.int32()),
        pa.field("eco", pa.string()),
        pa.field("opening", pa.string()),
        pa.field("time_control", pa.string(), nullable=False),
        pa.field("base_seconds", pa.int32()),
        pa.field("increment_seconds", pa.int32()),
        pa.field("termination", pa.string(), nullable=False),
        pa.field("movetext", pa.string(), nullable=False),
        pa.field("ply_count", pa.int32(), nullable=False),
        pa.field("has_clock", pa.bool_(), nullable=False),
        pa.field("has_eval", pa.bool_(), nullable=False),
    ]
)
_FIELDS = SCHEMA.names
_GAME_FIELDS = [f for f in _FIELDS if f != "utc_datetime"]
_MONTH_RE = re.compile(r"\d{4}-(0[1-9]|1[0-2])")


EXIT_DATA_ERROR = 3


class BronzeError(RuntimeError):
    pass


class DataError(BronzeError):
    """The input itself is bad: re-streaming the same immutable file fails the same way."""


def month_filename(month: str) -> str:
    return f"lichess_db_standard_rated_{month}.pgn.zst"


class HashingReader:
    """File-like wrapper that hashes and counts bytes as the decompressor pulls them."""

    def __init__(self, raw: BinaryIO) -> None:
        self._raw = raw
        self.sha256 = hashlib.sha256()
        self.bytes_read = 0

    def read(self, size: int = -1) -> bytes:
        chunk = self._raw.read(size)
        self.sha256.update(chunk)
        self.bytes_read += len(chunk)
        return chunk

    def drain(self) -> None:
        """Hash whatever the decompressor didn't need (zstd can stop before the last byte)."""
        while self.read(1 << 20):
            pass


# A 30 GB month over a home link sees stalls and resets; one of them shouldn't throw away
# hours of streaming. Each drop reopens at the next byte with a Range request (D-030).
MAX_RESUMES = 50
RESUME_BACKOFF_MAX_S = 60.0
_RETRYABLE = (OSError, http.client.HTTPException)  # timeouts, resets, TLS, short reads


class ResumingReader:
    """File-like over `open_at(offset)` that reopens at the current offset when a read
    fails or the body ends early. Bytes come out contiguous and exactly once, so the
    hash and the decompressor downstream never see the reconnect."""

    def __init__(
        self,
        open_at: Callable[[int], BinaryIO],
        length: int,
        first: BinaryIO | None = None,
        max_resumes: int = MAX_RESUMES,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._open_at = open_at
        self._length = length
        self._resp = first if first is not None else open_at(0)
        self._max = max_resumes
        self._sleep = sleep
        self.pos = 0
        self.resumes = 0

    def read(self, size: int = -1) -> bytes:
        while True:
            try:
                chunk = self._resp.read(size)
            except _RETRYABLE as e:
                self._resume(e)
                continue
            # Without a length an empty read is EOF; with one, it's EOF only at the end.
            if chunk or not self._length or self.pos >= self._length:
                self.pos += len(chunk)
                return chunk
            self._resume(None)

    def _resume(self, err: BaseException | None) -> None:
        if not self._length:
            raise BronzeError("connection dropped and the length is unknown") from err
        while True:
            self.resumes += 1
            if self.resumes > self._max:
                raise BronzeError(
                    f"gave up after {self._max} resumes at byte {self.pos:,}"
                ) from err
            wait = min(RESUME_BACKOFF_MAX_S, 2.0 ** min(self.resumes, 6))
            log.warning(
                "stream dropped at byte %s of %s (%s); resume %d in %.0f s",
                f"{self.pos:,}",
                f"{self._length:,}",
                type(err).__name__ if err else "early EOF",
                self.resumes,
                wait,
            )
            try:
                self._resp.close()
            except _RETRYABLE:
                pass
            self._sleep(wait)
            try:
                self._resp = self._open_at(self.pos)
                return
            except _RETRYABLE as e:
                err = e

    def close(self) -> None:
        self._resp.close()


def open_range(url: str, start: int) -> BinaryIO:
    headers = {"User-Agent": USER_AGENT}
    if start:
        headers["Range"] = f"bytes={start}-"
    resp = urllib.request.urlopen(
        urllib.request.Request(url, headers=headers), timeout=60
    )
    if start:
        # A server that ignores Range sends the file from byte 0; splicing that in would
        # corrupt the stream (the checksum would catch it, hours later). Fail now.
        cr = resp.headers.get("Content-Range", "")
        if resp.status != 206 or not cr.startswith(f"bytes {start}-"):
            resp.close()
            raise BronzeError(
                f"resume at {start} got HTTP {resp.status} Content-Range {cr!r}"
            )
    return resp


def published_sha256(filename: str, url: str = CHECKSUM_URL) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        for line in resp.read().decode().splitlines():
            parts = line.split()
            if len(parts) == 2 and parts[1] == filename:
                return parts[0].lower()
    raise BronzeError(f"{filename} not listed in {url}")


@dataclass
class BronzeStats:
    month: str
    source: str
    games: int
    malformed: int
    malformed_reasons: dict[str, int]
    malformed_examples: list[tuple[int, str]]
    stray_lines: int
    lines_read: int
    files: int
    bytes_read: int
    sha256: str
    checksum_verified: bool
    runtime_seconds: float


class _PartWriter:
    """Rolls Parquet files every ROWS_PER_FILE rows, row groups of ROWS_PER_BATCH."""

    def __init__(self, out_dir: Path) -> None:
        self.out_dir = out_dir
        self.files = 0
        self._writer: pq.ParquetWriter | None = None
        self._rows_in_file = 0
        self._cols: dict[str, list] = {f: [] for f in _FIELDS}
        self._buffered = 0

    def add(self, game: Game) -> None:
        for f in _GAME_FIELDS:
            self._cols[f].append(getattr(game, f))
        self._cols["utc_datetime"].append(
            datetime.combine(game.utc_date, game.utc_time, tzinfo=UTC)
        )
        self._buffered += 1
        if self._buffered >= ROWS_PER_BATCH:
            self._flush()

    def _flush(self) -> None:
        if not self._buffered:
            return
        if self._writer is None:
            path = self.out_dir / f"part-{self.files:05d}.parquet"
            self._writer = pq.ParquetWriter(path, SCHEMA, compression="zstd")
            self.files += 1
        batch = pa.record_batch(
            [pa.array(self._cols[f], SCHEMA.field(f).type) for f in _FIELDS],
            schema=SCHEMA,
        )
        self._writer.write_batch(batch, row_group_size=ROWS_PER_BATCH)
        self._rows_in_file += self._buffered
        self._cols = {f: [] for f in _FIELDS}
        self._buffered = 0
        if self._rows_in_file >= ROWS_PER_FILE:
            self._close_file()

    def _close_file(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None
            self._rows_in_file = 0

    def close(self) -> None:
        self._flush()
        self._close_file()


def write_partition(
    month: str,
    raw: BinaryIO,
    out_root: Path,
    expected_sha256: str | None,
    source: str,
    max_malformed_rate: float = MAX_MALFORMED_RATE,
) -> BronzeStats:
    """Parse one month's compressed bytes into `out_root/month=YYYY-MM/`, atomically.

    `expected_sha256=None` skips verification (local test files only); the computed hash
    is still recorded. Raises BronzeError and leaves any existing partition untouched if
    the checksum, malformed rate, or stream fails.
    """
    started = time.monotonic()
    out_root.mkdir(parents=True, exist_ok=True)
    final = out_root / f"month={month}"
    tmp = out_root / f".tmp-month={month}-{os.getpid()}"
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir()

    try:
        hashing = HashingReader(raw)
        parser = PgnParser()
        writer = _PartWriter(tmp)
        with text_stream(hashing) as text:
            for game in parser.iter_games(text):
                writer.add(game)
            hashing.drain()
        writer.close()
        stats: ParseStats = parser.stats

        digest = hashing.sha256.hexdigest()
        if expected_sha256 is not None and digest != expected_sha256:
            raise BronzeError(
                f"sha256 mismatch for {month}: got {digest}, expected {expected_sha256}"
            )
        total = stats.games_ok + stats.games_malformed
        if stats.games_ok == 0:
            raise DataError(
                f"no valid games in {month} (malformed: {stats.malformed_reasons})"
            )
        if stats.games_malformed / total > max_malformed_rate:
            raise DataError(
                f"malformed rate {stats.games_malformed}/{total} over {max_malformed_rate:.2%} "
                f"for {month}: {stats.malformed_reasons}"
            )

        result = BronzeStats(
            month=month,
            source=source,
            games=stats.games_ok,
            malformed=stats.games_malformed,
            malformed_reasons=stats.malformed_reasons,
            malformed_examples=stats.malformed_examples[:20],
            stray_lines=stats.stray_lines,
            lines_read=stats.lines_read,
            files=writer.files,
            bytes_read=hashing.bytes_read,
            sha256=digest,
            checksum_verified=expected_sha256 is not None,
            runtime_seconds=round(time.monotonic() - started, 2),
        )
        # `_` prefix: Spark and PyArrow skip it when reading the partition as data.
        (tmp / "_stats.json").write_text(json.dumps(asdict(result), indent=2) + "\n")
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise

    swap_in(tmp, final)
    return result


def swap_in(tmp: Path, final: Path) -> None:
    """Replace `final` with `tmp`. POSIX can't rename onto a non-empty dir, so move the
    old one aside first; the gap between the two renames is the only window in which
    the partition is missing."""
    old = final.with_name(f".old-{final.name}-{os.getpid()}")
    if final.exists():
        final.rename(old)
    tmp.rename(final)
    shutil.rmtree(old, ignore_errors=True)


def is_current(month: str, out_root: Path, expected_sha256: str) -> bool:
    """True if the partition on disk was built from a verified copy of this exact file."""
    try:
        stats = json.loads((out_root / f"month={month}" / "_stats.json").read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return False
    return (
        stats.get("checksum_verified") is True
        and stats.get("sha256") == expected_sha256
    )


def check_disk(out_root: Path, needed_bytes: int) -> None:
    out_root.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(out_root).free
    if free < needed_bytes:
        raise BronzeError(
            f"need ~{needed_bytes / 1e9:.1f} GB free at {out_root}, have {free / 1e9:.1f} GB"
        )


def run(month: str, source: str, out_root: Path, verify: bool = True) -> BronzeStats:
    if not _MONTH_RE.fullmatch(month):
        raise DataError(f"month must be YYYY-MM, got {month!r}")
    filename = month_filename(month)

    if source == "url":
        url = f"{BASE_URL}/{filename}"
        expected = published_sha256(filename) if verify else None
        first = open_range(url, 0)
        length = int(first.headers.get("Content-Length", 0))
        reader = ResumingReader(lambda start: open_range(url, start), length, first)
        try:
            check_disk(out_root, int(length * DISK_HEADROOM))
            log.info("streaming %s (%.1f MB)", url, length / 1e6)
            stats = write_partition(month, reader, out_root, expected, source=url)
        finally:
            reader.close()
        if reader.resumes:
            log.info("stream finished after %d resumes", reader.resumes)
        # http.client raises IncompleteRead on a short body; this catches a server that
        # closes cleanly early. The checksum would catch it too, unless verify is off.
        if length and stats.bytes_read != length:
            raise BronzeError(f"read {stats.bytes_read} of {length} bytes from {url}")
        return stats

    path = Path(source)
    if verify and path.name != filename:
        raise BronzeError(
            f"{path.name} isn't the Lichess file for {month}; pass --no-verify for local test files"
        )
    expected = published_sha256(filename) if verify else None
    check_disk(out_root, int(path.stat().st_size * DISK_HEADROOM))
    with path.open("rb") as f:
        return write_partition(month, f, out_root, expected, source=str(path))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m lichess.bronze", description=__doc__.split("\n")[0]
    )
    ap.add_argument("--month", required=True, help="YYYY-MM")
    ap.add_argument(
        "--source", default="url", help="'url' (default) or a local .pgn.zst path"
    )
    ap.add_argument("--out", type=Path, default=Path("data/bronze"))
    ap.add_argument(
        "--no-verify", action="store_true", help="skip the published SHA-256 check"
    )
    ap.add_argument(
        "--skip-if-current",
        action="store_true",
        help="do nothing if the partition's _stats.json has the published SHA-256",
    )
    args = ap.parse_args(argv)
    if args.skip_if_current and args.no_verify:
        ap.error("--skip-if-current needs the published checksum; drop --no-verify")
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    # The parser logs every malformed game; the summary in _stats.json is enough here.
    logging.getLogger("lichess.parser").setLevel(logging.ERROR)

    try:
        if args.skip_if_current:
            expected = published_sha256(month_filename(args.month))
            if is_current(args.month, args.out, expected):
                log.info("month=%s already matches %s, skipping", args.month, expected)
                print(
                    json.dumps(
                        {"status": "skipped", "month": args.month, "sha256": expected}
                    )
                )
                return 0
        stats = run(args.month, args.source, args.out, verify=not args.no_verify)
    except DataError as e:
        log.error("%s", e)
        return EXIT_DATA_ERROR
    except BronzeError as e:
        log.error("%s", e)
        return 1
    summary = {"status": "written"} | {
        k: v for k, v in asdict(stats).items() if k != "malformed_examples"
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
