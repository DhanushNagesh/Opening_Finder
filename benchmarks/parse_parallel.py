"""Single-process parser vs. chunked process pool, games/sec on one local .pgn.zst.

    uv run python benchmarks/parse_parallel.py data/raw/sample_2024-09/head100MB.pgn.zst --workers 1 2 4 8

Modes:
  single       PgnParser in this process (what bronze does today)
  pool-games   chunks parsed in N workers, Game objects pickled back to the parent
               (what a single bronze writer would need)
  pool-count   same, but workers return only a count: the ceiling if each worker
               wrote its own Parquet
The parent always decompresses and splits lines, so that cost is in every mode.
"""

import argparse
import os
import platform
import time
from collections.abc import Callable

from lichess.models import ParseStats
from lichess.parallel import (
    count_chunk,
    iter_chunks,
    iter_games_parallel,
    map_bounded,
)
from lichess.parser import PgnParser, open_pgn


def single(path: str, _workers: int) -> int:
    with open_pgn(path) as f:
        return sum(1 for _ in PgnParser().iter_games(f))


def pool_games(path: str, workers: int) -> int:
    with open_pgn(path) as f:
        return sum(1 for _ in iter_games_parallel(f, workers, ParseStats()))


def pool_count(path: str, workers: int) -> int:
    with open_pgn(path) as f:
        return sum(n for n, _, _ in map_bounded(count_chunk, iter_chunks(f), workers))


MODES: dict[str, Callable[[str, int], int]] = {
    "single": single,
    "pool-games": pool_games,
    "pool-count": pool_count,
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8])
    ap.add_argument("--repeat", type=int, default=2, help="runs per cell; best is kept")
    args = ap.parse_args()

    mb = os.path.getsize(args.path) / 1e6
    print(
        f"input={args.path} compressed_mb={mb:.1f} cpus={os.cpu_count()} "
        f"python={platform.python_version()} machine={platform.machine()}"
    )
    print("| Mode | Workers | Games | Best seconds | Games/sec | Compressed MB/s |")
    print("|---|---|---|---|---|---|")
    cells = [("single", 1)] + [
        (m, w) for m in ("pool-games", "pool-count") for w in args.workers
    ]
    for mode, workers in cells:
        best, games = float("inf"), 0
        for _ in range(args.repeat):
            start = time.perf_counter()
            games = MODES[mode](args.path, workers)
            best = min(best, time.perf_counter() - start)
        print(
            f"| {mode} | {workers} | {games:,} | {best:.2f} | "
            f"{games / best:,.0f} | {mb / best:.1f} |",
            flush=True,
        )


if __name__ == "__main__":
    main()
