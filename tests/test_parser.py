import io
from datetime import date, time
from pathlib import Path

import chess.pgn
import pytest
import zstandard

from lichess.parser import MalformedGame, PgnParser, count_plies, open_pgn

FIXTURES = Path(__file__).parent / "fixtures"


def parse_text(text: str) -> tuple[list, PgnParser]:
    parser = PgnParser()
    games = list(parser.iter_games(io.StringIO(text)))
    return games, parser


def parse_fixture(name: str):
    parser = PgnParser()
    with open_pgn(FIXTURES / name) as f:
        games = list(parser.iter_games(f))
    assert parser.stats.games_malformed == 0, parser.stats
    assert len(games) == 1
    return games[0]


def pgn(site_id: str = "abcdefgh", movetext: str = "1. e4 e5 1-0", **extra: str) -> str:
    headers = {
        "Event": "Rated Blitz game",
        "Site": f"https://lichess.org/{site_id}",
        "White": "a",
        "Black": "b",
        "Result": "1-0",
        "UTCDate": "2013.02.01",
        "UTCTime": "00:00:00",
        "WhiteElo": "1500",
        "BlackElo": "1500",
        "ECO": "C20",
        "Opening": "King's Pawn Game",
        "TimeControl": "300+0",
        "Termination": "Normal",
    }
    headers.update(extra)
    head = "".join(f'[{k} "{v}"]\n' for k, v in headers.items())
    return f"{head}\n{movetext}\n\n"


# (fixture, expected field values)
FIXTURE_CASES = [
    (
        "2013_missing_elo.pgn",
        {
            "game_id": "4zb5mv7a",
            "white": "?",
            "white_elo": None,
            "black_elo": 988,
            "white_rating_diff": None,
            "result": "0-1",
            "termination": "Time forfeit",
            "ply_count": 38,
            "base_seconds": 60,
            "increment_seconds": 0,
            "has_clock": False,
            "has_eval": False,
        },
    ),
    (
        "2013_correspondence.pgn",
        {
            "game_id": "bm1ouv8p",
            "time_control": "-",
            "base_seconds": None,
            "increment_seconds": None,
            "ply_count": 82,
            "white_rating_diff": 31,
            "black_rating_diff": -31,
        },
    ),
    (
        "2013_time_forfeit.pgn",
        {
            "game_id": "9tp6v4ps",
            "termination": "Time forfeit",
            "utc_date": date(2013, 1, 31),
            "utc_time": time(23, 1, 50),
            "ply_count": 33,
            "base_seconds": 480,
        },
    ),
    (
        "2013_draw.pgn",
        {
            "game_id": "7xms8zzm",
            "result": "1/2-1/2",
            "ply_count": 26,
            "white_rating_diff": -17,
        },
    ),
    (
        "2013_eval_comments.pgn",
        {"game_id": "vq7by3h2", "ply_count": 23, "has_eval": True, "has_clock": False},
    ),
    (
        "2013_no_rating_diff.pgn",
        {
            "game_id": "i5elh7m6",
            "black_elo": None,
            "white_rating_diff": None,
            "black_rating_diff": None,
            "ply_count": 17,
            "increment_seconds": 8,
        },
    ),
    (
        "2024_abandoned_zero_moves.pgn",
        {
            "game_id": "jF3xnsFH",
            "termination": "Abandoned",
            "ply_count": 0,
            "eco": None,
            "opening": None,
            "movetext": " 0-1",
            "has_clock": False,
        },
    ),
    (
        "2024_abandoned_one_move.pgn",
        {
            "game_id": "dKPz9R0T",
            "termination": "Abandoned",
            "ply_count": 1,
            "has_clock": True,
        },
    ),
    (
        "2024_zero_moves_time_forfeit.pgn",
        {
            "game_id": "RC8ZuZvw",
            "termination": "Time forfeit",
            "ply_count": 0,
            "white_rating_diff": None,
            "event": "Classical swiss https://lichess.org/swiss/9pszh8Hd",
        },
    ),
    (
        "2024_rules_infraction.pgn",
        {"game_id": "26gvtWgY", "termination": "Rules infraction", "ply_count": 15},
    ),
    (
        "2024_clk_eval_mate.pgn",
        {"game_id": "FqwXiMEE", "ply_count": 40, "has_clock": True, "has_eval": True},
    ),
    (
        "2024_correspondence.pgn",
        {
            "game_id": "sX5SL2HD",
            "base_seconds": None,
            "ply_count": 44,
            "has_clock": False,
        },
    ),
    ("2024_titled_bot.pgn", {"game_id": "9aRLVJ1P", "ply_count": 2, "black_elo": 2070}),
    (
        "2024_draw_clk.pgn",
        {
            "game_id": "N058lz0n",
            "result": "1/2-1/2",
            "ply_count": 33,
            "white_rating_diff": 0,
            "black_rating_diff": 0,
        },
    ),
]


@pytest.mark.parametrize(
    ("name", "expected"), FIXTURE_CASES, ids=[c[0] for c in FIXTURE_CASES]
)
def test_fixture(name, expected):
    game = parse_fixture(name)
    actual = {k: getattr(game, k) for k in expected}
    assert actual == expected


def test_every_fixture_is_covered():
    singles = {p.name for p in FIXTURES.glob("*.pgn")} - {"2013-02_first_500.pgn"}
    assert singles == {name for name, _ in FIXTURE_CASES}


def test_missing_elo_is_none():
    game = parse_fixture("2013_missing_elo.pgn")
    assert game.white_elo is None and game.black_elo == 988


def test_abandoned_zero_moves_has_zero_plies():
    game = parse_fixture("2024_abandoned_zero_moves.pgn")
    assert game.termination == "Abandoned" and game.ply_count == 0


def test_correspondence_time_control_is_none():
    game = parse_fixture("2013_correspondence.pgn")
    assert (game.base_seconds, game.increment_seconds) == (None, None)


def test_games_without_blank_line_between_them_split():
    text = pgn("aaaaaaaa").rstrip("\n") + "\n" + pgn("bbbbbbbb")
    assert "1-0\n[Event" in text
    games, parser = parse_text(text)
    assert [g.game_id for g in games] == ["aaaaaaaa", "bbbbbbbb"]
    assert parser.stats.games_malformed == 0


def test_missing_blank_between_headers_and_movetext():
    text = pgn().replace('"Normal"]\n\n', '"Normal"]\n')
    games, _ = parse_text(text)
    assert len(games) == 1 and games[0].ply_count == 2


def test_escaped_quotes_in_headers():
    text = pgn(White=r"the \"best\" player", Event=r"Rated \"Blitz\" game \\o/")
    games, _ = parse_text(text)
    assert games[0].white == 'the "best" player'
    assert games[0].event == 'Rated "Blitz" game \\o/'


def test_truncated_final_game_is_malformed():
    text = pgn("aaaaaaaa") + pgn("bbbbbbbb", movetext="1. e4 e5 2. Nf3 { [%clk 0:0")
    text = text.rstrip("\n")
    games, parser = parse_text(text)
    assert [g.game_id for g in games] == ["aaaaaaaa"]
    assert parser.stats.games_malformed == 1
    assert parser.stats.malformed_reasons == {"unbalanced_brackets": 1}


def test_truncated_at_move_boundary_is_malformed():
    games, parser = parse_text(pgn(movetext="1. e4 e5 2. Nf3").rstrip("\n"))
    assert games == []
    assert parser.stats.malformed_reasons == {"no_result_token": 1}


def test_truncated_mid_header_is_malformed():
    text = (
        pgn("aaaaaaaa") + '[Event "Rated Blitz game"]\n[Site "https://lichess.org/bbbb'
    )
    games, parser = parse_text(text)
    assert len(games) == 1
    assert parser.stats.malformed_reasons == {"bad_header_line": 1}


def test_headers_without_movetext_then_next_game():
    first = pgn("aaaaaaaa").split("\n\n")[0] + "\n\n"
    games, parser = parse_text(first + pgn("bbbbbbbb"))
    assert [g.game_id for g in games] == ["bbbbbbbb"]
    assert parser.stats.malformed_reasons == {"no_movetext": 1}


def test_missing_required_header():
    text = pgn().replace('[Termination "Normal"]\n', "")
    games, parser = parse_text(text)
    assert games == []
    assert parser.stats.malformed_examples[0][1] == "missing_header:Termination"


def test_result_mismatch_is_malformed():
    games, parser = parse_text(pgn(movetext="1. e4 e5 0-1"))
    assert games == [] and parser.stats.malformed_reasons == {"result_mismatch": 1}


def test_bad_elo_is_malformed_and_parsing_continues():
    games, parser = parse_text(pgn("aaaaaaaa", WhiteElo="abc") + pgn("bbbbbbbb"))
    assert [g.game_id for g in games] == ["bbbbbbbb"]
    assert parser.stats.malformed_reasons == {"bad_value": 1}
    assert parser.stats.games_ok == 1


def test_multiline_movetext():
    games, _ = parse_text(pgn(movetext="1. e4 e5\n2. Nf3 Nc6\n3. Bb5 1-0"))
    assert games[0].ply_count == 5
    assert games[0].movetext == "1. e4 e5\n2. Nf3 Nc6\n3. Bb5 1-0"


def test_stray_lines_before_first_game_are_counted():
    games, parser = parse_text("garbage\n\n" + pgn())
    assert len(games) == 1 and parser.stats.stray_lines == 1


@pytest.mark.parametrize(
    ("movetext", "plies"),
    [
        (" 0-1", 0),
        ("1. e4 1-0", 1),
        ("1.e4 e5 2.Nf3 1-0", 3),
        ("1. e4 $1 e5 $14 1-0", 2),
        ("1. e4 { a (comment) } e5 1-0", 2),
        ("1. e4 (1. d4 d5 (1... Nf6 2. c4)) 1... e5 1/2-1/2", 2),
        ("1. e4 e5 ; rest of line\n2. Nf3 *", 3),
        ("1. e4?! { [%eval 0.2] [%clk 0:01:00] } 1... e5?? { [%eval #-2] } 0-1", 2),
    ],
)
def test_count_plies(movetext, plies):
    assert count_plies(movetext)[0] == plies


def test_count_plies_rejects_missing_result():
    with pytest.raises(MalformedGame):
        count_plies("1. e4 e5")


def test_reads_zstd_stream(tmp_path):
    raw = (FIXTURES / "2013-02_first_500.pgn").read_bytes()
    path = tmp_path / "sample.pgn.zst"
    path.write_bytes(zstandard.ZstdCompressor().compress(raw))
    parser = PgnParser()
    with open_pgn(path) as f:
        games = list(parser.iter_games(f))
    assert len(games) == 500
    assert parser.stats.games_malformed == 0


def test_ply_count_matches_python_chess_on_500_games():
    path = FIXTURES / "2013-02_first_500.pgn"
    parser = PgnParser()
    with open_pgn(path) as f:
        ours = list(parser.iter_games(f))

    reference = []
    with path.open(encoding="utf-8") as f:
        while (game := chess.pgn.read_game(f)) is not None:
            assert not game.errors, game.errors
            site = game.headers["Site"]
            reference.append((site.rsplit("/", 1)[1], len(list(game.mainline_moves()))))

    assert len(reference) == 500
    assert [(g.game_id, g.ply_count) for g in ours] == reference


def test_stats_lines_read_when_consumer_stops_early():
    parser = PgnParser()
    games = parser.iter_games(io.StringIO(pgn("aaaaaaaa") + pgn("bbbbbbbb")))
    next(games)
    games.close()
    assert parser.stats.games_ok == 1
    assert parser.stats.lines_read == 16
