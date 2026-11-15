-- Openings ranked for each rating band, speed and colour by the Wilson lower bound on
-- score (D-007), among lines with at least var('min_games_best_openings') games (D-024).
-- Rolled up over ECO: 94 opening names carry more than one ECO code, and a user picks
-- an opening by name.
{% set min_games = var('min_games_best_openings') %}

with by_opening as (
    select
        rating_band,
        tc_bucket,
        color,
        opening_family,
        opening,
        string_agg(distinct eco, ', ' order by eco) as eco_codes,
        sum(games) as games,
        sum(wins) as wins,
        sum(draws) as draws,
        sum(losses) as losses,
        sum(avg_ply * games) / sum(games) as avg_ply
    from {{ ref('fct_opening_performance') }}
    group by rating_band, tc_bucket, color, opening_family, opening
    having sum(games) >= {{ min_games }}
),

scored as (
    select
        *,
        (wins + 0.5 * draws) / games as score
    from by_opening
)

select
    rating_band,
    tc_bucket,
    color,
    row_number() over (
        partition by rating_band, tc_bucket, color
        order by {{ wilson_lower_bound('score', 'games') }} desc, games desc, opening
    ) as rank,
    opening_family,
    opening,
    eco_codes,
    games,
    wins,
    draws,
    losses,
    score,
    {{ wilson_lower_bound('score', 'games') }} as score_wilson_lb,
    avg_ply
from scored
