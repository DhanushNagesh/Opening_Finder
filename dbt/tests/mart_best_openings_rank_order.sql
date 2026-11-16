-- Rank 1 has the highest Wilson bound in its slice, and ranks have no gaps.
select *
from (
    select
        *,
        lag(score_wilson_lb) over w as prev_lb,
        lag(rank) over w as prev_rank
    from {{ ref('mart_best_openings') }}
    window w as (partition by rating_band, tc_bucket, color order by rank)
)
where (prev_lb is not null and score_wilson_lb > prev_lb)
   or (prev_rank is not null and rank != prev_rank + 1)
   or (prev_rank is null and rank != 1)
