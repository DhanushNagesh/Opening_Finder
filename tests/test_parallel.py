import io
from pathlib import Path

import pytest

from lichess.models import ParseStats
from lichess.parallel import iter_chunks, iter_games_parallel
from lichess.parser import PgnParser

FIXTURES = Path(__file__).parent / "fixtures"


def _awkward_stream() -> str:
    """Every fixture, plus the cases that stress chunk boundaries: stray lines before
    the first game, two games with no blank line between them, a malformed game in the
    middle, and a final game cut mid-movetext."""
    parts = ["stray text before any game\n\n"]
    parts += [p.read_text() for p in sorted(FIXTURES.glob("*.pgn"))]
    first_500 = (FIXTURES / "2013-02_first_500.pgn").read_text()
    games = first_500.split("\n\n[Event ")
    # Two games with no blank line between them.
    parts.append("[Event " + games[2].rstrip("\n") + "\n[Event " + games[3] + "\n\n")
    parts.append(
        '[Event "Rated Blitz game"]\n[Site "https://lichess.org/badelo01"]\n'
        '[WhiteElo "abc"]\n\n1. e4 e5 1-0\n\n'
    )
    # Ends mid-movetext: no result token, no trailing newline.
    parts.append(games[4].split("\n\n")[0] + "\n\n1. e4 e5 2. Nf3")
    return "".join(parts)


def _lines(text: str):
    return io.StringIO(text, newline="")


def test_chunks_reassemble_and_start_at_event() -> None:
    text = _awkward_stream()
    chunks = list(iter_chunks(_lines(text), games_per_chunk=7))
    assert "".join(t for _, t in chunks) == text
    assert all(t.startswith("[Event ") for _, t in chunks[1:])
    # Offsets are the running line count, so malformed line numbers can be made absolute.
    lines_before = 0
    for offset, t in chunks:
        assert offset == lines_before
        lines_before += len(list(_lines(t)))


@pytest.mark.parametrize("games_per_chunk", [1, 3, 7, 10_000])
def test_parallel_matches_single_process(games_per_chunk: int) -> None:
    text = _awkward_stream()
    single = PgnParser()
    expected = list(single.iter_games(_lines(text)))

    stats = ParseStats()
    got = list(
        iter_games_parallel(
            _lines(text), workers=2, stats=stats, games_per_chunk=games_per_chunk
        )
    )

    assert got == expected
    s = single.stats
    assert (
        stats.games_ok,
        stats.games_malformed,
        stats.lines_read,
        stats.stray_lines,
    ) == (s.games_ok, s.games_malformed, s.lines_read, s.stray_lines)
    assert stats.malformed_reasons == s.malformed_reasons
    assert [line for line, _ in stats.malformed_examples] == [
        line for line, _ in s.malformed_examples
    ]
    assert s.games_malformed >= 2  # the bad Elo game and the truncated tail
