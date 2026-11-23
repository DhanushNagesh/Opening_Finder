import pytest

import finder


@pytest.fixture(scope="module")
def gold():
    return finder.load()


def test_rating_band():
    assert finder.rating_band(1200) == 1200
    assert finder.rating_band(1299) == 1200
    assert finder.rating_band(999) == 900


def test_best_openings_ranked_by_wilson(gold):
    best = finder.best_openings(gold, 1200, "blitz", "white", top=5)
    assert best["rank"].tolist() == [1, 2, 3, 4, 5]
    assert best.score_wilson_lb.is_monotonic_decreasing
    assert (best.games >= 100).all()


def test_most_blunders_respects_cutoff(gold):
    b = finder.most_blunders(gold, 1200, "blitz", "white")
    assert len(b) == 10
    assert (b.games_with_eval >= finder.MIN_ANALYSED_GAMES).all()
    assert b.blunders_per_game.is_monotonic_decreasing


def test_slice_totals_add_up_to_meta(gold):
    assert gold.slices.games.sum() == gold.meta["games"]
    assert gold.slices.games_with_eval.sum() == gold.meta["games_with_eval"]
    games, analysed = finder.slice_totals(gold, 1200, "blitz")
    assert 0 < analysed < games


def test_empty_slice(gold):
    assert finder.slice_totals(gold, 100, "blitz") == (0, 0)
    assert finder.best_openings(gold, 100, "blitz", "white").empty


def test_month_runs():
    assert finder.month_runs(["2024-09"]) == "2024-09"
    assert (
        finder.month_runs(["2013-02", "2013-01", "2012-12", "2024-08", "2024-10"])
        == "2012-12 to 2013-02, 2024-08 and 2024-10"
    )
