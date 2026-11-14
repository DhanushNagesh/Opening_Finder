"""Streaming PGN parser for Lichess monthly dumps.

Reads one line at a time, so memory stays constant regardless of file size. It does
not check move legality; python-chess does that in the tests only (D-003).
"""

import io
import logging
import re
from collections.abc import Iterable, Iterator
from datetime import date, time
from pathlib import Path
from typing import BinaryIO, TextIO

import zstandard

from lichess.models import Game, ParseStats

log = logging.getLogger(__name__)

# Greedy value match: everything up to the final `"]`, so escaped quotes inside
# the value don't end it early. `\w+` keeps `[%clk ...]` comment text from matching.
_HEADER_RE = re.compile(r'\[(\w+)\s+"(.*)"\]\s*$')
_SITE_RE = re.compile(r"https://lichess\.org/([A-Za-z0-9]{8})")
_COMMENT_RE = re.compile(r"\{[^}]*\}")
_LINE_COMMENT_RE = re.compile(r";[^\n]*")
_VARIATION_RE = re.compile(r"\([^()]*\)")
_MOVE_NUMBER_RE = re.compile(r"\d+\.+")

RESULTS = frozenset({"1-0", "0-1", "1/2-1/2", "*"})
REQUIRED_HEADERS = (
    "Event",
    "Site",
    "White",
    "Black",
    "Result",
    "UTCDate",
    "UTCTime",
    "WhiteElo",
    "BlackElo",
    "ECO",
    "Opening",
    "TimeControl",
    "Termination",
)


class MalformedGame(ValueError):
    pass


def text_stream(raw: BinaryIO) -> TextIO:
    """Wrap a binary zstd stream (file or HTTP body) as decompressed UTF-8 text."""
    reader = zstandard.ZstdDecompressor().stream_reader(raw, read_across_frames=True)
    return io.TextIOWrapper(reader, encoding="utf-8", newline="")


def open_pgn(path: str | Path) -> TextIO:
    """Open a `.pgn.zst` (streamed) or plain `.pgn` file as text."""
    path = Path(path)
    if path.suffix == ".zst":
        return text_stream(path.open("rb"))
    return path.open(encoding="utf-8", newline="")


def count_plies(movetext: str) -> tuple[int, str]:
    """Return (ply count, result token). Raises MalformedGame if there's no result token."""
    text = movetext
    if "{" in text:
        text = _COMMENT_RE.sub(" ", text)
    if ";" in text:
        text = _LINE_COMMENT_RE.sub(" ", text)
    if "(" in text:
        # innermost first, until nested variations are gone
        while True:
            stripped = _VARIATION_RE.sub(" ", text)
            if stripped == text:
                break
            text = stripped
    if "{" in text or "}" in text or "(" in text or ")" in text:
        raise MalformedGame("unbalanced_brackets")
    tokens = _MOVE_NUMBER_RE.sub(" ", text).split()
    if not tokens or tokens[-1] not in RESULTS:
        raise MalformedGame("no_result_token")
    plies = sum(1 for t in tokens[:-1] if t[0] != "$")
    return plies, tokens[-1]


def _elo(value: str) -> int | None:
    return None if value == "?" else int(value)


def _optional_int(value: str | None) -> int | None:
    return None if value is None else int(value)


def _unknown_to_none(value: str) -> str | None:
    return None if value == "?" else value


def _time_control(value: str) -> tuple[int | None, int | None]:
    if value == "-":
        return None, None
    base, inc = value.split("+")
    return int(base), int(inc)


def build_game(headers: dict[str, str], movetext: str) -> Game:
    """Turn one game's headers and movetext into a Game. Raises MalformedGame."""
    for key in REQUIRED_HEADERS:
        if key not in headers:
            raise MalformedGame(f"missing_header:{key}")

    site = headers["Site"]
    m = _SITE_RE.fullmatch(site)
    if not m:
        raise MalformedGame(f"bad_site:{site}")

    plies, result_token = count_plies(movetext)
    result = headers["Result"]
    if result_token != result:
        raise MalformedGame(f"result_mismatch:{result}!={result_token}")

    try:
        d = headers["UTCDate"]
        t = headers["UTCTime"]
        base, inc = _time_control(headers["TimeControl"])
        return Game(
            game_id=m.group(1),
            event=headers["Event"],
            site=site,
            utc_date=date(int(d[0:4]), int(d[5:7]), int(d[8:10])),
            utc_time=time(int(t[0:2]), int(t[3:5]), int(t[6:8])),
            white=headers["White"],
            black=headers["Black"],
            result=result,
            white_elo=_elo(headers["WhiteElo"]),
            black_elo=_elo(headers["BlackElo"]),
            white_rating_diff=_optional_int(headers.get("WhiteRatingDiff")),
            black_rating_diff=_optional_int(headers.get("BlackRatingDiff")),
            eco=_unknown_to_none(headers["ECO"]),
            opening=_unknown_to_none(headers["Opening"]),
            time_control=headers["TimeControl"],
            base_seconds=base,
            increment_seconds=inc,
            termination=headers["Termination"],
            movetext=movetext,
            ply_count=plies,
            has_clock="[%clk" in movetext,
            has_eval="[%eval" in movetext,
        )
    except ValueError as e:
        raise MalformedGame(f"bad_value:{e}") from e


def _unescape(value: str) -> str:
    return value.replace('\\"', '"').replace("\\\\", "\\")


class PgnParser:
    """State machine: idle -> headers -> (blank) -> movetext -> idle.

    A game ends at a blank line after movetext, at the next header line (even with no
    blank line before it), or at end of input. `stats` is reset on each `iter_games`
    call and is complete once the iterator is exhausted.
    """

    def __init__(self) -> None:
        self.stats = ParseStats()

    def iter_games(self, stream: Iterable[str]) -> Iterator[Game]:
        stats = self.stats = ParseStats()
        headers: dict[str, str] = {}
        move_lines: list[str] = []
        header_error: str | None = None
        start_line = 0
        # 0 idle, 1 in headers, 2 blank seen after headers, 3 in movetext
        state = 0

        def finish() -> Game | None:
            try:
                if header_error:
                    raise MalformedGame(header_error)
                if not move_lines:
                    raise MalformedGame("no_movetext")
                game = build_game(headers, "\n".join(move_lines))
            except MalformedGame as e:
                site = headers.get("Site", "?")
                log.warning("malformed game at line %d (%s): %s", start_line, site, e)
                stats.record_malformed(start_line, str(e))
                return None
            stats.games_ok += 1
            return game

        lineno = 0
        # finally: keeps lines_read right when the consumer stops early
        try:
            for raw in stream:
                lineno += 1
                line = raw.rstrip()

                if not line:
                    if state == 3:
                        if (game := finish()) is not None:
                            yield game
                        state = 0
                    elif state == 1:
                        state = 2
                    continue

                m = _HEADER_RE.match(line) if line[0] == "[" else None
                if m:
                    key = m.group(1)
                    # A header after movetext, after the post-header blank, or a repeated key
                    # means a new game started.
                    if state in (2, 3) or (state == 1 and key in headers):
                        if (game := finish()) is not None:
                            yield game
                        state = 0
                    if state == 0:
                        headers, move_lines, header_error = {}, [], None
                        start_line = lineno
                        state = 1
                    value = m.group(2)
                    headers[key] = _unescape(value) if "\\" in value else value
                    continue

                if state == 0:
                    stats.stray_lines += 1
                    continue
                if state == 1 and line[0] == "[":
                    header_error = header_error or f"bad_header_line:{lineno}"
                    continue
                move_lines.append(line)
                state = 3

        finally:
            stats.lines_read = lineno
        if state != 0 and (game := finish()) is not None:
            yield game
