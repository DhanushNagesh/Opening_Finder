-- Blunders by opening family, rating band, speed and the colour that blundered, from
-- analysed games only (D-008): a small, self-selected subset, not all games.
-- A blunder is a move after which the eval drops by >= 300 cp for the side that made
-- it, with every eval clamped to +/-1000 cp so mate scores and won positions can't
-- produce huge swings (D-023). Ply 1 has no eval before it and is never scored.
{% set cap = 1000 %}
{% set threshold = 300 %}

with analysed as (
    select game_id, opening_family, rating_band, tc_bucket, evals_cp
    from {{ ref('stg_games') }}
    where has_eval
),

plies as (
    select
        game_id,
        ply,
        {{ clamp('evals_cp[ply - 1]', cap) }} as before_cp,
        {{ clamp('evals_cp[ply]', cap) }} as after_cp
    from analysed,
        unnest(generate_series(2, len(evals_cp))) as t(ply)
),

blunders as (
    select
        game_id,
        case when ply % 2 = 1 then 'white' else 'black' end as color,
        (ply + 1) // 2 as move_number
    from plies
    where before_cp is not null
      and after_cp is not null
      and case when ply % 2 = 1 then before_cp - after_cp else after_cp - before_cp end
          >= {{ threshold }}
),

per_game_side as (
    select
        a.opening_family,
        a.rating_band,
        a.tc_bucket,
        c.color,
        count(b.game_id) filter (where b.move_number <= 10) as blunders_opening,
        count(b.game_id) filter (where b.move_number between 11 and 25) as blunders_middlegame,
        count(b.game_id) filter (where b.move_number >= 26) as blunders_late
    from analysed as a
    cross join (values ('white'), ('black')) as c(color)
    left join blunders as b
        on b.game_id = a.game_id and b.color = c.color
    group by a.game_id, a.opening_family, a.rating_band, a.tc_bucket, c.color
)

select
    opening_family,
    rating_band,
    tc_bucket,
    color,
    count(*) as games_with_eval,
    sum(blunders_opening + blunders_middlegame + blunders_late) as blunders,
    avg(blunders_opening + blunders_middlegame + blunders_late) as blunders_per_game,
    avg(blunders_opening) as blunders_per_game_moves_1_10,
    avg(blunders_middlegame) as blunders_per_game_moves_11_25,
    avg(blunders_late) as blunders_per_game_moves_26_plus
from per_game_side
group by all
