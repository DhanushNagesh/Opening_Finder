---
title: Opening Finder
emoji: ♟
colorFrom: gray
colorTo: green
sdk: static
pinned: false
short_description: Best Lichess openings for your rating, from 273M games
---

# Opening Finder

Enter a Lichess rating, time control and colour to see the openings that score best in that rating band, ranked by the Wilson lower bound on score, and the opening families with the most blunders per game among computer-analysed games.

Data: 273M rated games from the Lichess open database (2013-01 to 2013-06, 2024-08 to 2024-10), parsed from the raw PGN dumps and aggregated with Spark and dbt-duckdb. The `data/` folder here is the exported gold layer, 4.9 MB.
