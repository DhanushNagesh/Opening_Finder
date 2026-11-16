-- One row per opening line, rating band, speed and colour. Each game appears twice,
-- once from each side, so the White and Black rows of a cell mirror each other.
with by_side as (
    select
        opening_family, opening, eco, rating_band, tc_bucket,
        'white' as color, white_score as score, ply_count
    from {{ ref('stg_games') }}
    union all
    select
        opening_family, opening, eco, rating_band, tc_bucket,
        'black' as color, 1 - white_score as score, ply_count
    from {{ ref('stg_games') }}
),

agg as (
    select
        opening_family,
        opening,
        eco,
        rating_band,
        tc_bucket,
        color,
        count(*) as games,
        count(*) filter (where score = 1) as wins,
        count(*) filter (where score = 0.5) as draws,
        count(*) filter (where score = 0) as losses,
        avg(score) as score,
        avg(ply_count) as avg_ply
    from by_side
    group by all
)

select
    *,
    {{ wilson_lower_bound('score', 'games') }} as score_wilson_lb
from agg
