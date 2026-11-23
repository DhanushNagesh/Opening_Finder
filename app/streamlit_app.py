import streamlit as st

import finder

st.set_page_config(page_title="Opening Finder", page_icon="♟", layout="wide")


@st.cache_resource
def gold() -> finder.Gold:
    return finder.load()


g = gold()
lo, hi = finder.bands(g)
meta = g.meta
month_list = finder.month_runs([m["month"] for m in meta["months"]])

st.title("Opening Finder")
st.caption(
    "Which openings score best at your rating on Lichess, ranked by the Wilson lower bound "
    "on score so a line with 40 lucky games can't beat one with 40,000 solid ones."
)

c1, c2, c3 = st.columns(3)
rating = c1.number_input(
    "Your rating", min_value=lo, max_value=hi + 99, value=1200, step=50
)
speed = c2.selectbox("Time control", finder.SPEEDS, index=finder.SPEEDS.index("blitz"))
color = c3.radio("You play", finder.COLORS, horizontal=True, format_func=str.capitalize)

band = finder.rating_band(int(rating))
games, analysed = finder.slice_totals(g, band, speed)

st.markdown(
    f"**{band}–{band + 99} {speed}, as {color}.** Based on {games:,} rated games in this band "
    f"(games bucketed by the two players' average rating), from the {len(meta['months'])} "
    f"monthly dumps {month_list}. Openings need at least 100 games to be ranked."
)

best = finder.best_openings(g, band, speed, color)
st.subheader("Best openings")
st.caption(
    "An opening is named after the position the game reached, not who steered it there. "
    "As Black, many rows are White's choices: read them as lines to hope for or to "
    "prepare against, not lines you can force."
    if color == "black"
    else "An opening is named after the position the game reached; Black's replies "
    "decide part of it, so not every row is a line you can force."
)
if best.empty:
    st.info(
        "No opening has 100+ games in this slice. Try a neighbouring rating or speed."
    )
else:
    st.dataframe(
        best[
            [
                "rank",
                "opening",
                "eco_codes",
                "games",
                "score",
                "score_wilson_lb",
                "avg_ply",
            ]
        ],
        hide_index=True,
        width="stretch",
        column_config={
            "rank": "Rank",
            "opening": "Opening",
            "eco_codes": "ECO",
            "games": st.column_config.NumberColumn("Games", format="%d"),
            "score": st.column_config.NumberColumn(
                "Score", format="%.3f", help="(wins + ½ draws) / games, from your side"
            ),
            "score_wilson_lb": st.column_config.NumberColumn(
                "Wilson lower bound",
                format="%.3f",
                help="95% lower bound on score; the ranking key",
            ),
            "avg_ply": st.column_config.NumberColumn("Avg plies", format="%.0f"),
        },
    )

st.subheader("Most blunders per game — analysed games only")
st.warning(
    f"Only {analysed:,} of the {games:,} games in this slice have computer analysis. "
    "Players choose which games to analyse, so this is a self-selected sample, not a "
    "random one. A blunder is a move that loses at least 3 pawns of evaluation for the "
    "side that made it (evals capped at ±10)."
)
blunders = finder.most_blunders(g, band, speed, color)
if blunders.empty:
    st.info("No opening family has 100+ analysed games in this slice.")
else:
    st.dataframe(
        blunders[
            [
                "opening_family",
                "games_with_eval",
                "blunders_per_game",
                "blunders_per_game_moves_1_10",
                "blunders_per_game_moves_11_25",
                "blunders_per_game_moves_26_plus",
            ]
        ],
        hide_index=True,
        width="stretch",
        column_config={
            "opening_family": "Opening family",
            "games_with_eval": st.column_config.NumberColumn(
                "Analysed games", format="%d"
            ),
            "blunders_per_game": st.column_config.NumberColumn(
                f"Your blunders / game (as {color})", format="%.2f"
            ),
            "blunders_per_game_moves_1_10": st.column_config.NumberColumn(
                "Moves 1–10", format="%.2f"
            ),
            "blunders_per_game_moves_11_25": st.column_config.NumberColumn(
                "Moves 11–25", format="%.2f"
            ),
            "blunders_per_game_moves_26_plus": st.column_config.NumberColumn(
                "Moves 26+", format="%.2f"
            ),
        },
    )

st.caption(
    f"{meta['games']:,} games from the Lichess open database, parsed from raw PGN."
)
