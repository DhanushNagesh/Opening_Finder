"""Cross-check one silver month against the Hugging Face Lichess Parquet (D-001).

    uv run python -I scripts/hf_crosscheck.py data/silver/games/month=2013-02 \
        data/raw/hf_2013-02/train-00000-of-00001.parquet

The HF file is downloaded data: this script only reads it as Parquet.
"""

import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

SCORE = {"1-0": 1.0, "0-1": 0.0, "1/2-1/2": 0.5}


def white_score(results: pa.ChunkedArray) -> pa.Array:
    return pa.array([SCORE.get(r) for r in results.to_pylist()], pa.float64())


def by_opening(
    table: pa.Table, opening_col: str, result_col: str
) -> dict[str, tuple[int, float]]:
    t = pa.table(
        {"opening": table[opening_col], "score": white_score(table[result_col])}
    )
    g = t.group_by("opening").aggregate([("score", "count"), ("score", "mean")])
    return {
        o: (n, s)
        for o, n, s in zip(
            g["opening"].to_pylist(),
            g["score_count"].to_pylist(),
            g["score_mean"].to_pylist(),
            strict=True,
        )
    }


def main(silver_dir: str, hf_file: str) -> int:
    silver = ds.dataset(silver_dir, format="parquet").to_table(
        columns=[
            "game_id",
            "result",
            "opening",
            "white_elo",
            "black_elo",
            "time_control",
        ]
    )
    hf = pq.read_table(
        hf_file,
        columns=["Site", "Result", "Opening", "WhiteElo", "BlackElo", "TimeControl"],
    )
    hf = hf.append_column("game_id", pc.utf8_slice_codeunits(hf["Site"], 20))

    print(f"rows: silver {silver.num_rows:,}  hf {hf.num_rows:,}")
    s_ids, h_ids = set(silver["game_id"].to_pylist()), set(hf["game_id"].to_pylist())
    print(f"distinct game_id: silver {len(s_ids):,}  hf {len(h_ids):,}  ")
    print(f"only in silver: {len(s_ids - h_ids)}  only in hf: {len(h_ids - s_ids)}")

    joined = silver.join(hf, "game_id", join_type="inner")
    pairs = [
        ("result", "Result"),
        ("opening", "Opening"),
        ("white_elo", "WhiteElo"),
        ("black_elo", "BlackElo"),
        ("time_control", "TimeControl"),
    ]
    for ours, theirs in pairs:
        a = joined[ours].to_pylist()
        b = joined[theirs].to_pylist()
        if ours == "opening":  # silver stores "?" as null
            b = [None if v == "?" else v for v in b]
        diff = sum(x != y for x, y in zip(a, b, strict=True))
        print(f"column {ours:<12} mismatches on joined rows: {diff}")

    s_open = by_opening(silver, "opening", "result")
    h_open = by_opening(hf, "Opening", "Result")
    top = sorted(s_open, key=lambda o: -s_open[o][0])[:3]
    print("\nwhite score by opening (top 3 by games)")
    print(f"{'opening':<40} {'silver n':>9} {'hf n':>9} {'silver':>8} {'hf':>8}")
    for o in top:
        (sn, ss), (hn, hs) = s_open[o], h_open[o]
        print(f"{o:<40} {sn:>9,} {hn:>9,} {ss:>8.4f} {hs:>8.4f}")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
