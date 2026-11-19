"""Parse one decompressed PGN stream with a process pool (PLAN phase 2 task 2, D-027).

zstd isn't splittable, so one reader in the parent decompresses and cuts the text at
`[Event ` lines into chunks of whole games; workers run the normal `PgnParser` on each
chunk. At most `2 * workers` chunks are in flight, so memory stays bounded on a 30 GB
month instead of the reader racing ahead (`Pool.imap` would consume the whole input).
"""

import io
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future, ProcessPoolExecutor
from typing import TypeVar

from lichess.models import Game, ParseStats
from lichess.parser import PgnParser

GAMES_PER_CHUNK = 20_000
T = TypeVar("T")


def iter_chunks(
    lines: Iterable[str], games_per_chunk: int = GAMES_PER_CHUNK
) -> Iterator[tuple[int, str]]:
    """Yield (first line number - 1, text) chunks that each end just before an
    `[Event ` line, so no game is split. Lines before the first `[Event ` (stray text)
    go into the first chunk, where the parser counts them as it would anyway."""
    buf: list[str] = []
    games = 0
    offset = 0
    for line in lines:
        if line.startswith("[Event "):
            if games == games_per_chunk:
                yield offset, "".join(buf)
                offset += len(buf)
                buf, games = [], 0
            games += 1
        buf.append(line)
    if buf:
        yield offset, "".join(buf)


def parse_chunk(chunk: tuple[int, str]) -> tuple[list[Game], ParseStats, int]:
    offset, text = chunk
    parser = PgnParser()
    # newline="" splits lines exactly as the reader's TextIOWrapper did; splitlines()
    # would also split on \u2028 and friends inside a name or comment.
    games = list(parser.iter_games(io.StringIO(text, newline="")))
    return games, parser.stats, offset


def count_chunk(chunk: tuple[int, str]) -> tuple[int, ParseStats, int]:
    """Parse but return only the count: the ceiling if workers wrote their own output
    instead of shipping Game objects back to the parent."""
    games, stats, offset = parse_chunk(chunk)
    return len(games), stats, offset


def merge_stats(total: ParseStats, part: ParseStats, line_offset: int) -> None:
    total.games_ok += part.games_ok
    total.games_malformed += part.games_malformed
    total.lines_read += part.lines_read
    total.stray_lines += part.stray_lines
    for k, v in part.malformed_reasons.items():
        total.malformed_reasons[k] = total.malformed_reasons.get(k, 0) + v
    room = ParseStats.MAX_EXAMPLES - len(total.malformed_examples)
    # Start lines are made absolute; a line number inside the reason text (e.g.
    # `bad_header_line:12`) stays relative to its chunk.
    total.malformed_examples.extend(
        (line + line_offset, reason) for line, reason in part.malformed_examples[:room]
    )


def map_bounded(
    fn: Callable[[tuple[int, str]], T],
    chunks: Iterable[tuple[int, str]],
    workers: int,
    in_flight: int | None = None,
) -> Iterator[T]:
    """Ordered `executor.map` that never holds more than `in_flight` pending chunks."""
    in_flight = in_flight or 2 * workers
    with ProcessPoolExecutor(max_workers=workers) as pool:
        pending: deque[Future[T]] = deque()
        for chunk in chunks:
            pending.append(pool.submit(fn, chunk))
            if len(pending) >= in_flight:
                yield pending.popleft().result()
        while pending:
            yield pending.popleft().result()


def iter_games_parallel(
    lines: Iterable[str],
    workers: int,
    stats: ParseStats,
    games_per_chunk: int = GAMES_PER_CHUNK,
) -> Iterator[Game]:
    """Same games, in the same order, as `PgnParser().iter_games(lines)`. `stats` is
    filled in as chunks complete and is final once the iterator is exhausted."""
    for games, part, offset in map_bounded(
        parse_chunk, iter_chunks(lines, games_per_chunk), workers
    ):
        merge_stats(stats, part, offset)
        yield from games
