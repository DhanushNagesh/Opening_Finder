"""Single-core parse throughput on one local .pgn.zst file.

    uv run python benchmarks/parse_throughput.py data/raw/lichess_db_standard_rated_2013-02.pgn.zst --mode parse

Modes: `decompress` (zstd + UTF-8 line iteration only, the floor), `parse` (our parser),
`python-chess` (chess.pgn.read_game, first --limit games). Run one mode per process so
peak RSS is per mode. With `-`, peak RSS includes the buffered input.

A 2024 sample, streamed and never saved (the last game is cut, so expect 1 malformed):

    curl -s -r 0-49999999 https://database.lichess.org/standard/lichess_db_standard_rated_2024-01.pgn.zst \
      | uv run python benchmarks/parse_throughput.py - --mode parse
"""

import argparse
import io
import platform
import resource
import sys
import time
from itertools import islice

from lichess.parser import PgnParser, open_pgn, text_stream


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("path", help="file, or - to read zstd bytes from stdin into memory")
    ap.add_argument(
        "--mode", choices=["decompress", "parse", "python-chess"], required=True
    )
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    if args.path == "-":
        # Buffer first so the timing excludes the network; nothing is written to disk.
        data = io.BytesIO(sys.stdin.buffer.read())
        print(f"stdin_compressed_mb={len(data.getbuffer()) / 1e6:.1f}")
        source = text_stream(data)
    else:
        source = open_pgn(args.path)

    start = time.perf_counter()
    with source as f:
        if args.mode == "decompress":
            games = sum(1 for line in f if line.startswith("[Event "))
        elif args.mode == "parse":
            parser = PgnParser()
            games = sum(1 for _ in islice(parser.iter_games(f), args.limit))
        else:
            import chess.pgn

            games = 0
            while (args.limit is None or games < args.limit) and chess.pgn.read_game(f):
                games += 1
    elapsed = time.perf_counter() - start

    # ru_maxrss is bytes on macOS, KiB on Linux
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rss_mb = rss / 2**20 if sys.platform == "darwin" else rss / 2**10
    print(
        f"mode={args.mode} games={games} seconds={elapsed:.2f} "
        f"games_per_sec={games / elapsed:,.0f} peak_rss_mb={rss_mb:.0f} "
        f"python={platform.python_version()} machine={platform.machine()}"
    )
    if args.mode == "parse":
        print(f"stats={parser.stats}")


if __name__ == "__main__":
    main()
