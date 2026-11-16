-- Analysed games only (D-008): a biased subset. Both colours and all speeds pooled,
-- since 1200-1299 has just 69 analysed games in 2013 H1.
select opening_family,
       sum(games_with_eval) / 2 as analysed_games,
       sum(blunders) as blunders,
       round(sum(blunders) / (sum(games_with_eval) / 2), 2) as blunders_per_game,
       round(sum(blunders_per_game_moves_1_10 * games_with_eval) / (sum(games_with_eval) / 2), 2) as per_game_moves_1_10,
       round(sum(blunders_per_game_moves_11_25 * games_with_eval) / (sum(games_with_eval) / 2), 2) as per_game_moves_11_25,
       round(sum(blunders_per_game_moves_26_plus * games_with_eval) / (sum(games_with_eval) / 2), 2) as per_game_moves_26_plus
from {{ ref('fct_opening_blunders') }}
where rating_band = 1200
group by opening_family
having sum(games_with_eval) / 2 >= 5
order by blunders_per_game desc
limit 10
