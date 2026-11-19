-- Games that count for opening stats: not abandoned, at least 2 full moves, both
-- players rated. 0.2% of 2013 games have a `?` Elo and so no rating band; every gold
-- model groups by band, so they'd only show up as a null key. Games with result `*`
-- (Termination `Unterminated`: correspondence games still running when the month was
-- dumped, 23,054 in 2024-09, none in 2013) have no score, so they're dropped too (D-029).
select
    game_id,
    month,
    utc_date,
    white_elo,
    black_elo,
    rating_band,
    tc_bucket,
    opening_family,
    opening,
    eco,
    white_score,
    ply_count,
    has_eval,
    evals_cp
from {{ source('silver', 'games') }}
where is_decisive_play
  and rating_band is not null
  and white_score is not null
