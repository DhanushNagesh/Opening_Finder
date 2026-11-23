-- Blunders by opening family, rating band, speed and the colour that blundered, from
-- analysed games only (D-008): a small, self-selected subset, not all games.
-- A blunder is a move after which the eval drops by >= 300 cp for the side that made
-- it, with every eval clamped to +/-1000 cp so mate scores and won positions can't
-- produce huge swings (D-023). Ply 1 has no eval before it and is never scored.
--
-- Each game's eval list is scored in place with a list lambda: no unnest to one row per
-- ply and no join back to the game. The unnest version made DuckDB decorrelate the
-- lateral join (group the games, lists included, then hash-join 1.6B plies back) and
-- took 584 s on three 2024 months; see D-031.
{% set cap = 1000 %}
{% set threshold = 300 %}

with analysed as (
    select opening_family, rating_band, tc_bucket, evals_cp
    from {{ ref('stg_games') }}
    where has_eval
),

-- Plies (2..n) after which the mover's eval fell by at least the threshold. Odd plies
-- are White's moves, even plies Black's.
per_game as (
    select
        opening_family,
        rating_band,
        tc_bucket,
        list_filter(
            range(2, len(evals_cp) + 1),
            lambda p: ({{ clamp('evals_cp[p - 1]', cap) }}) is not null
                and ({{ clamp('evals_cp[p]', cap) }}) is not null
                and case
                    when p % 2 = 1
                        then {{ clamp('evals_cp[p - 1]', cap) }} - {{ clamp('evals_cp[p]', cap) }}
                    else {{ clamp('evals_cp[p]', cap) }} - {{ clamp('evals_cp[p - 1]', cap) }}
                end >= {{ threshold }}
        ) as blunder_plies
    from analysed
),

per_game_side as (
    select
        g.opening_family,
        g.rating_band,
        g.tc_bucket,
        c.color,
        -- Move number of ply p is (p + 1) // 2.
        len(list_filter(g.blunder_plies, lambda p: p % 2 = c.parity and (p + 1) // 2 <= 10))
            as blunders_opening,
        len(list_filter(g.blunder_plies, lambda p: p % 2 = c.parity and (p + 1) // 2 between 11 and 25))
            as blunders_middlegame,
        len(list_filter(g.blunder_plies, lambda p: p % 2 = c.parity and (p + 1) // 2 >= 26))
            as blunders_late
    from per_game as g
    cross join (values ('white', 1), ('black', 0)) as c(color, parity)
)

select
    opening_family,
    rating_band,
    tc_bucket,
    color,
    count(*) as games_with_eval,
    sum(blunders_opening + blunders_middlegame + blunders_late)::bigint as blunders,
    avg(blunders_opening + blunders_middlegame + blunders_late) as blunders_per_game,
    avg(blunders_opening) as blunders_per_game_moves_1_10,
    avg(blunders_middlegame) as blunders_per_game_moves_11_25,
    avg(blunders_late) as blunders_per_game_moves_26_plus
from per_game_side
group by all
