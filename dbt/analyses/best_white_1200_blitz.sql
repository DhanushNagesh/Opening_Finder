select rank, opening, eco_codes, games, wins, draws, losses,
       round(score, 3) as score, round(score_wilson_lb, 3) as wilson_lb
from {{ ref('mart_best_openings') }}
where rating_band = 1200 and tc_bucket = 'blitz' and color = 'white'
order by rank
limit 5
