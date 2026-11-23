"""Export the gold tables the demo app reads to small Parquet files under app/data/.

Writes three files, each overwritten whole:
  best_openings.parquet  mart_best_openings, sorted by band, speed, colour, rank
  blunders.parquet       fct_opening_blunders, sorted by band, speed, colour, family
  slices.parquet         games and analysed games per band and speed (all openings)
  meta.json              months covered and game counts, for the app's sample-size line

No timestamps go into the output, so rerunning on the same gold gives the same files.
"""

import argparse
import json
from pathlib import Path

import duckdb

DEFAULT_DB = Path("data/gold/gold.duckdb")
DEFAULT_OUT = Path("app/data")

BEST_OPENINGS_SQL = """
    select rating_band, tc_bucket, color, rank, opening_family, opening, eco_codes,
           games::bigint as games, wins::bigint as wins, draws::bigint as draws,
           losses::bigint as losses, score, score_wilson_lb, avg_ply
    from mart_best_openings
    order by rating_band, tc_bucket, color, rank
"""

BLUNDERS_SQL = """
    select rating_band, tc_bucket, color, opening_family, games_with_eval, blunders,
           blunders_per_game, blunders_per_game_moves_1_10,
           blunders_per_game_moves_11_25, blunders_per_game_moves_26_plus
    from fct_opening_blunders
    order by rating_band, tc_bucket, color, opening_family
"""

# Every game appears once per colour in both fact tables, so count one colour.
SLICES_SQL = """
    select p.rating_band, p.tc_bucket, p.games, coalesce(b.games_with_eval, 0) as games_with_eval
    from (
        select rating_band, tc_bucket, sum(games)::bigint as games
        from fct_opening_performance where color = 'white' group by all
    ) as p
    left join (
        select rating_band, tc_bucket, sum(games_with_eval)::bigint as games_with_eval
        from fct_opening_blunders where color = 'white' group by all
    ) as b using (rating_band, tc_bucket)
    order by rating_band, tc_bucket
"""

META_SQL = """
    select month, count(*) as games, count(*) filter (where has_eval) as games_with_eval,
           min(utc_date)::varchar as first_date, max(utc_date)::varchar as last_date
    from stg_games
    group by month
    order by month
"""


def export(db_path: Path, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        for name, sql in [
            ("best_openings", BEST_OPENINGS_SQL),
            ("blunders", BLUNDERS_SQL),
            ("slices", SLICES_SQL),
        ]:
            path = out_dir / f"{name}.parquet"
            con.execute(
                f"copy ({sql}) to '{path}' (format parquet, compression zstd, row_group_size 100000)"
            )
        months = [
            dict(
                zip(
                    ("month", "games", "games_with_eval", "first_date", "last_date"),
                    row,
                )
            )
            for row in con.execute(META_SQL).fetchall()
        ]
    finally:
        con.close()

    meta = {
        "months": months,
        "games": sum(m["games"] for m in months),
        "games_with_eval": sum(m["games_with_eval"] for m in months),
        "first_date": min(m["first_date"] for m in months),
        "last_date": max(m["last_date"] for m in months),
    }
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    return meta


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", type=Path, default=DEFAULT_DB)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()
    meta = export(args.db, args.out)
    for f in sorted(args.out.iterdir()):
        print(f"{f}  {f.stat().st_size / 1e6:.2f} MB")
    print(
        f"{meta['games']:,} games, {meta['games_with_eval']:,} with evals, "
        f"{len(meta['months'])} months"
    )


if __name__ == "__main__":
    main()
