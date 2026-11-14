# Data dictionary — Lichess standard rated PGN

Measured on two samples with `scripts/profile_headers.py`. Raw profiler output is in
[`profile_2013-02.md`](profile_2013-02.md) and [`profile_2024-01_sample.md`](profile_2024-01_sample.md).

| Sample | Source | Games |
|---|---|---|
| 2013-02 | full file, 18.2 MB compressed, SHA-256 `c136acdf…2fd86` matches Lichess | 123,961 |
| 2024-01 | first 50,000,000 compressed bytes (`curl -r 0-49999999`), streamed, nothing saved | 155,057 (UTCTime 00:00–01:51 on 2024-01-01) |

The 2024 sample covers only the first hours of New Year's Day, so its rates for rare
fields (titles, correspondence, abandonment) are indicative, not monthly averages.
The final game in the byte range is cut mid-header. That one game accounts for the
single unparseable header line, the one missing `Termination`, and the one
`no_movetext_line` in the 2024 profile.

Checksums: `https://database.lichess.org/standard/sha256sums.txt`, one line per file,
format `<sha256>  <filename>`. Bronze (phase 2) verifies against this.

## Headers

"Null" means the value is `?`, or the key is absent where noted. Rates are % of games.

| Key | Type | Meaning | 2013-02 null | 2024-01 null | How we use it |
|---|---|---|---|---|---|
| `Event` | string | Game category, plus tournament/swiss URL if any. E.g. `Rated Blitz game`, `Rated Bullet tournament https://…`, `Blitz swiss https://…` | 0 | 0 | Bronze only. **Not** used for speed class (see drift below). Could flag tournament vs casual pool later. |
| `Site` | string | `https://lichess.org/<id>` | 0 | 0 | `game_id` = last path segment. Unique within each sample (distinct = games). |
| `Date` | `YYYY.MM.DD` | Game date | absent (key new) | 0 | Bronze only; equals `UTCDate` in the sample. Use `UTCDate`. |
| `Round` | string | Always `-` | absent (key new) | 0 (always `-`) | Dropped. |
| `White`, `Black` | string | Username | `?` 0.08% / 0.13% | 0 | Bronze only. `?` = account gone; always paired with `?` Elo. |
| `Result` | enum | `1-0`, `0-1`, `1/2-1/2` | 0 | 0 | `white_score` 1/0/0.5. `*` never seen; still map it to null. |
| `UTCDate` | `YYYY.MM.DD` | Start date, UTC | 0 | 0 | `utc_date`. **Not** the partition key: 166 games in the 2013-02 file are dated 2013.01.31. |
| `UTCTime` | `HH:MM:SS` | Start time, UTC | 0 | 0 | `utc_time`. |
| `WhiteElo`, `BlackElo` | int or `?` | Rating before the game | `?` 0.08% / 0.13% | 0 | `int \| None`. `rating_band` needs both, so `?` games fall out of gold. Range 800–2357 in 2013-02. Spike at exactly `1500` (Glicko starting rating, provisional players). |
| `WhiteRatingDiff`, `BlackRatingDiff` | signed int (`+5`, `-20`, `+0`) | Rating change | **absent** 0.21% | **absent** 0.23% | `int \| None`. 2013: absent in exactly the 265 games with a `?` Elo. 2024: absent in some swiss games and a few pool games, with no `?` Elo involved; cause unknown. Not needed for gold. |
| `WhiteTitle`, `BlackTitle` | enum | `GM IM FM CM NM WGM WIM WFM WCM LM BOT` | **absent** 99.98% | **absent** 99.3% | Bronze only. `BOT` (~0.2% per side in 2024) = engine account; candidate filter for gold, logged later if used. |
| `ECO` | `A00`–`E99` or `?` | Opening code | 0 | `?` 0.14% | `eco`. `?` exactly when the game has zero moves. |
| `Opening` | string or `?` | `Family: Variation, Sub-variation` | 0 | `?` 0.14% | `opening`; `opening_family` = text before first `:`. Same `?` rule as ECO. |
| `TimeControl` | `base+inc` (seconds) or `-` | Clock | 0 | 0 | `base_seconds`, `increment_seconds`; `-` (correspondence) → both null. `tc_bucket` is computed from this. |
| `Termination` | enum | `Normal`, `Time forfeit`, `Abandoned`, `Rules infraction` | 0 | 0 | Kept; `Abandoned` → `is_decisive_play = false`. 2013-02 has only `Normal` and `Time forfeit`. |

Header order is fixed within a period (Event, Site, [Date, Round,] White, Black, Result,
UTCDate, UTCTime, WhiteElo, BlackElo, [RatingDiffs], [Titles], ECO, Opening, TimeControl,
Termination). The parser must not depend on it, because optional keys move the rest.

## Movetext

| Feature | 2013-02 | 2024-01 | Notes |
|---|---|---|---|
| Comments `{ … }` | 0.37% | 99.79% | Only ever hold `[%clk]` and/or `[%eval]` in both samples. |
| `[%clk H:MM:SS]` | 0 | 99.76% | Clock after the move. Absent on zero-move games. |
| `[%eval x]` | 0.37% | 8.50% | Already present in 2013. Formats: `0.2`, `-1.35` (**pawns, not centipawns**), `#3`, `#-2` (mate in N). |
| Glyphs `?! ? ??` on moves | 0.37% | 8.49% | Only seen in eval games (Lichess analysis marks inaccuracy/mistake/blunder). Strip before counting plies. |
| NAGs `$n` | 0 | 0 | Not seen. |
| Variations `( … )` | 0 | 0 | Not seen. |
| Escaped `\"` in headers | 0 | 0 | Not seen. |
| Zero moves | 0 | 0.14% | Movetext line is just ` 0-1` (leading space). ECO/Opening are `?`. Mostly `Abandoned`, also seen with `Time forfeit` and `Normal`. |
| Movetext over multiple lines | 0 | 0 | Always one line. |
| Game without blank line before next `[Event` | 0 | 0 | Not seen. |

File layout confirmed: header lines, one blank line, one movetext line ending in the
result token, one blank line. The file ends with `\n\n`.

## Format drift 2013 → 2024

- **New keys:** `Date`, `Round`. `Title` values grow from `FM LM` to the full list, including `BOT`.
- **New Termination values:** `Abandoned`, `Rules infraction`.
- **New Event forms:** `Rated Rapid`, `Rated UltraBullet`, and swiss events (`Blitz swiss https://…`) that lack the word "Rated".
- **Event label semantics changed.** In 2013-02, 39,385 of 41,939 "Classical" games are *rapid* by the TimeControl formula. Lichess added the Rapid category later. Deriving `tc_bucket` from `TimeControl` gives one consistent definition across years; using `Event` would not.
- **Site id case:** 2013 ids are all lowercase; 2024 ids are mixed case (`iSjpt213`). `game_id` is case-sensitive, so never lower-case it.
- **Comments:** from rare (eval-only) to almost every move (`[%clk]`). 2024 movetext is several times larger per game, so bytes per game, not games, drive parse cost.
- **Missing Elo** (`?`) seen in 2013 only, always with a `?` username.

## Fragile points for the parser and silver

- Treating a game's month as `UTCDate`'s month moves 166 games out of the 2013-02 file. Partition by **source file month**. A game that starts on the last day of a month could also show up in the next month's file. Dedup is per month, so a duplicate across months would get through; phase 2 should check this once two adjacent months are loaded.
- Zero-move movetext ` 0-1` with a leading space: a tokenizer that assumes the line starts with `1.` miscounts or drops these games.
- An eval parser that assumes centipawn integers breaks on `0.2` and on `#-2`.
- Escaped quotes, variations, NAGs, missing blank lines, and multi-line movetext never appear in the samples. The parser still handles them per PLAN phase 1, with synthetic fixtures because there are no real examples.

## Fixtures (`tests/fixtures/`)

Each file holds one real game, copied verbatim, except `2013-02_first_500.pgn`.

| File | Case |
|---|---|
| `2013_missing_elo.pgn` | `?` White name and Elo, live game |
| `2013_correspondence.pgn` | `TimeControl "-"` |
| `2013_time_forfeit.pgn` | Time forfeit |
| `2013_draw.pgn` | `1/2-1/2` |
| `2013_eval_comments.pgn` | `[%eval]` comments including a mate score, glyphs |
| `2013_no_rating_diff.pgn` | RatingDiff keys absent |
| `2024_abandoned_zero_moves.pgn` | Abandoned, movetext ` 0-1`, ECO `?` |
| `2024_abandoned_one_move.pgn` | Abandoned after one ply |
| `2024_zero_moves_normal.pgn` | Zero moves, Termination `Time forfeit`, swiss Event, no RatingDiff |
| `2024_rules_infraction.pgn` | `Rules infraction` |
| `2024_clk_eval_mate.pgn` | `[%clk]` + `[%eval #N]` on every move |
| `2024_correspondence.pgn` | Correspondence with clocks |
| `2024_titled_bot.pgn` | `BOT` title |
| `2024_draw_clk.pgn` | Draw with clocks |
| `2013-02_first_500.pgn` | First 500 games of 2013-02, for the python-chess `ply_count` check (phase 1) |
