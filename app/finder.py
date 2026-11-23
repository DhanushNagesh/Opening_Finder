"""Lookups behind the demo. Pure pandas over the exported gold files, no Streamlit."""

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).parent / "data"
SPEEDS = ["ultrabullet", "bullet", "blitz", "rapid", "classical", "correspondence"]
COLORS = ["white", "black"]
# Same cutoff as mart_best_openings (D-024), applied to analysed games for blunders.
MIN_ANALYSED_GAMES = 100


@dataclass(frozen=True)
class Gold:
    best: pd.DataFrame
    blunders: pd.DataFrame
    slices: pd.DataFrame
    meta: dict


def load(data_dir: Path = DATA_DIR) -> Gold:
    return Gold(
        best=pd.read_parquet(data_dir / "best_openings.parquet"),
        blunders=pd.read_parquet(data_dir / "blunders.parquet"),
        slices=pd.read_parquet(data_dir / "slices.parquet"),
        meta=json.loads((data_dir / "meta.json").read_text()),
    )


def rating_band(rating: int) -> int:
    """Silver bands games by the floor-100 of the two players' average Elo."""
    return rating // 100 * 100


def _slice(
    df: pd.DataFrame, band: int, speed: str, color: str | None = None
) -> pd.DataFrame:
    mask = (df.rating_band == band) & (df.tc_bucket == speed)
    if color is not None:
        mask &= df.color == color
    return df[mask]


def best_openings(
    gold: Gold, band: int, speed: str, color: str, top: int = 10
) -> pd.DataFrame:
    return _slice(gold.best, band, speed, color).sort_values("rank").head(top)


def most_blunders(
    gold: Gold, band: int, speed: str, color: str, top: int = 10
) -> pd.DataFrame:
    s = _slice(gold.blunders, band, speed, color)
    s = s[s.games_with_eval >= MIN_ANALYSED_GAMES]
    return s.sort_values(
        ["blunders_per_game", "games_with_eval"], ascending=False
    ).head(top)


def slice_totals(gold: Gold, band: int, speed: str) -> tuple[int, int]:
    """(games, analysed games) in this band and speed, across all openings."""
    s = _slice(gold.slices, band, speed)
    if s.empty:
        return 0, 0
    return int(s.games.iloc[0]), int(s.games_with_eval.iloc[0])


def bands(gold: Gold) -> tuple[int, int]:
    return int(gold.slices.rating_band.min()), int(gold.slices.rating_band.max())


def month_runs(months: list[str]) -> str:
    """'2013-01 to 2013-03 and 2024-08' for consecutive YYYY-MM runs."""

    def idx(m: str) -> int:
        return int(m[:4]) * 12 + int(m[5:7])

    runs: list[list[str]] = []
    for m in sorted(months):
        if runs and idx(m) == idx(runs[-1][-1]) + 1:
            runs[-1].append(m)
        else:
            runs.append([m])
    parts = [r[0] if len(r) == 1 else f"{r[0]} to {r[-1]}" for r in runs]
    return ", ".join(parts[:-1]) + (" and " if len(parts) > 1 else "") + parts[-1]
