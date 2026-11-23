import json

import duckdb
import pyarrow.parquet as pq

from lichess.export_gold import export


def _gold_db(path):
    con = duckdb.connect(str(path))
    con.execute("""
        create table mart_best_openings as select * from (values
            (1200, 'blitz', 'white', 2, 'Italian Game', 'Italian Game: B', 'C50',
             200::hugeint, 100::hugeint, 20::hugeint, 80::hugeint, 0.55, 0.48, 60.0),
            (1200, 'blitz', 'white', 1, 'Italian Game', 'Italian Game: A', 'C50, C51',
             300::hugeint, 180::hugeint, 30::hugeint, 90::hugeint, 0.65, 0.59, 58.0)
        ) t(rating_band, tc_bucket, color, rank, opening_family, opening, eco_codes,
            games, wins, draws, losses, score, score_wilson_lb, avg_ply)
    """)
    con.execute("""
        create table fct_opening_blunders as select * from (values
            ('Italian Game', 1200, 'blitz', 'white', 40::bigint, 30::bigint, 0.75, 0.1, 0.4, 0.25)
        ) t(opening_family, rating_band, tc_bucket, color, games_with_eval, blunders,
            blunders_per_game, blunders_per_game_moves_1_10,
            blunders_per_game_moves_11_25, blunders_per_game_moves_26_plus)
    """)
    con.execute("""
        create table stg_games as select * from (values
            ('2013-01', date '2012-12-31', false),
            ('2013-01', date '2013-01-31', true),
            ('2024-09', date '2024-09-30', false)
        ) t(month, utc_date, has_eval)
    """)
    con.close()


def test_export_writes_sorted_tables_and_meta(tmp_path):
    db = tmp_path / "gold.duckdb"
    _gold_db(db)
    out = tmp_path / "out"
    meta = export(db, out)

    best = pq.read_table(out / "best_openings.parquet")
    assert best.column("rank").to_pylist() == [1, 2]
    assert str(best.schema.field("games").type) == "int64"
    assert pq.read_table(out / "blunders.parquet").num_rows == 1

    assert meta == json.loads((out / "meta.json").read_text())
    assert meta["games"] == 3
    assert meta["games_with_eval"] == 1
    assert (meta["first_date"], meta["last_date"]) == ("2012-12-31", "2024-09-30")
    assert [m["month"] for m in meta["months"]] == ["2013-01", "2024-09"]


def test_export_is_byte_identical_on_rerun(tmp_path):
    db = tmp_path / "gold.duckdb"
    _gold_db(db)
    a, b = tmp_path / "a", tmp_path / "b"
    export(db, a)
    export(db, b)
    for name in ("best_openings.parquet", "blunders.parquet", "meta.json"):
        assert (a / name).read_bytes() == (b / name).read_bytes()
