"""Counts and an aggregate fingerprint for one silver month, to compare reruns.

uv run python scripts/silver_summary.py data/silver/games/month=2013-02
"""

import hashlib
import json
import sys

import pyarrow.compute as pc
import pyarrow.dataset as ds


def main(silver_dir: str) -> int:
    t = ds.dataset(silver_dir, format="parquet").to_table(
        columns=[
            "game_id",
            "tc_bucket",
            "opening_family",
            "white_score",
            "is_decisive_play",
        ]
    )
    print(f"count:                   {t.num_rows:,}")
    print(f"count distinct game_id:  {pc.count_distinct(t['game_id']).as_py():,}")
    print(f"sum white_score:         {pc.sum(t['white_score']).as_py()}")
    print(f"is_decisive_play true:   {pc.sum(t['is_decisive_play']).as_py():,}")

    print("\ntc_bucket       games")
    g = t.group_by("tc_bucket").aggregate([("game_id", "count")]).sort_by("tc_bucket")
    for b, n in zip(
        g["tc_bucket"].to_pylist(), g["game_id_count"].to_pylist(), strict=True
    ):
        print(f"{b!s:<15} {n:>7,}")

    fam = t.group_by("opening_family").aggregate([("game_id", "count")])
    rows = sorted(
        zip(
            fam["opening_family"].to_pylist(),
            fam["game_id_count"].to_pylist(),
            strict=True,
        ),
        key=lambda r: str(r[0]),
    )
    digest = hashlib.sha256(json.dumps(rows).encode()).hexdigest()[:16]
    print(f"\nopening_family groups: {len(rows)}  fingerprint of counts: {digest}")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
