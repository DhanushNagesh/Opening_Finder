-- No ranked opening may sit below the minimum-games cutoff.
select *
from {{ ref('mart_best_openings') }}
where games < {{ var('min_games_best_openings') }}
