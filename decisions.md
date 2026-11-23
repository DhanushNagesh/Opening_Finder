# Decisions

One entry per decision you'd have to explain in an interview. Newest at the bottom. Don't edit old entries; if one gets reversed, add a new entry that supersedes it and mark the old one `Superseded by D-0xx`.

Format:

```
## D-0xx — Title
Date: YYYY-MM-DD · Phase: N · Status: Accepted | Proposed | Superseded by D-0yy
Context: what forced a choice.
Options: what was considered.
Decision: what we picked.
Why: the tradeoff, in one or two sentences.
Label: defensible | cargo-culted (and the simpler alternative)
Revisit if: the condition that would change the answer.
```

---

## D-001 — Parse raw PGN instead of using the Hugging Face Parquet
Date: 2026-11-13 · Phase: 0 · Status: Accepted
Context: Lichess publishes the same games as Parquet on Hugging Face, which would skip the parser entirely.
Options: (a) start from HF Parquet; (b) parse raw `.pgn.zst`.
Decision: (b). Use HF Parquet only to cross-check row counts and aggregates.
Why: Ingesting a non-splittable, semi-structured format is the main engineering problem in this project. Starting from Parquet removes it. Having an independent source to compare against is a bonus.
Label: defensible, as long as the README says the HF dataset exists and explains why we didn't use it.
Revisit if: the parser is still the bottleneck after phase 5 and the project needs many more months.

## D-002 — Stream zstd, don't decompress to disk
Date: 2026-11-13 · Phase: 1 · Status: Accepted
Context: 2024 months are ~30 GB compressed and much bigger decompressed. zstd frames aren't splittable for Spark.
Options: (a) `zstd -d` then let Spark read text; (b) stream through Python `zstandard` into the parser.
Decision: (b).
Why: Avoids several hundred GB of temporary disk per month, and Spark couldn't split the decompressed file on game boundaries anyway (a game spans many lines).
Label: defensible.
Revisit if: single-reader decompression becomes the ceiling. Then measure the chunked process pool (PLAN phase 2, task 2).

## D-003 — Own parser, python-chess only as a test oracle
Date: 2026-11-13 · Phase: 1 · Status: Accepted (phase 1: 60.6K vs 1.9K games/sec, ~32×, `benchmarks/results.md`)
Context: python-chess parses PGN correctly but replays every move on a board, which is slow at hundreds of millions of games.
Options: (a) python-chess everywhere; (b) a line-based state machine that doesn't validate move legality.
Decision: (b) for the pipeline, (a) in tests to check `ply_count` and splitting on a sample.
Why: We need headers, move count and eval/clock annotations, not legal-move validation. Lichess files are machine-generated, so illegal moves aren't a realistic risk.
Label: defensible, once the speed difference is measured and recorded.
Revisit if: a later feature needs board positions (e.g. opening by position instead of by name).

## D-004 — Bronze written with PyArrow, silver with Spark
Date: 2026-11-13 · Phase: 2 · Status: Accepted
Context: Parsing is plain Python per file; Spark would only add serialization overhead there.
Decision: PyArrow writes bronze directly from the parser. Spark starts at bronze → silver.
Why: Spark adds nothing to a single-stream, non-splittable read. It earns its place once data is columnar and splittable.
Label: defensible.
Revisit if: phase 5 shows DuckDB handles silver just as well at our data size, in which case Spark stays for the benchmark story and the README says so honestly.

## D-005 — Month partitions, overwritten atomically
Date: 2026-11-13 · Phase: 2 · Status: Accepted
Context: Reruns and backfills must not duplicate data.
Decision: Partition by `month=YYYY-MM`. Bronze writes to a temp dir and renames; Spark uses `partitionOverwriteMode=dynamic`. Dedup on `game_id` in silver as a second guard.
Why: Month is the unit of the source files, so it's also the natural unit of retry. Overwrite makes every task idempotent without needing a merge or upsert.
Label: defensible.
Revisit if: we ever ingest partial months (e.g. a live API feed), which would need upserts by `game_id`.

## D-006 — Gold in dbt on DuckDB, not on Spark
Date: 2026-11-13 · Phase: 4 · Status: Accepted
Context: Gold is aggregations over silver. dbt needs an adapter.
Options: dbt-spark (needs a Thrift server), dbt-duckdb reading Parquet.
Decision: dbt-duckdb.
Why: No server to run, reads Parquet in place, and the gold output is small. The Spark-vs-DuckDB benchmark in phase 5 gives the numbers to back this.
Label: defensible.
Revisit if: silver grows past what one machine's DuckDB handles in reasonable time.

## D-007 — Rank openings by Wilson lower bound
Date: 2026-11-13 · Phase: 4 · Status: Accepted
Context: Raw win rate puts tiny-sample openings at the top.
Decision: Rank by the Wilson score lower bound on `score` (draws count 0.5), plus a minimum-games cutoff N picked from the data.
Why: Penalises small samples without an arbitrary weight. Easy to explain.
Label: defensible. Fragile point: treating draws as half-wins in a binomial interval is an approximation; say so if asked.
Revisit if: we want per-player recommendations (would need a different model).

## D-008 — Blunder metrics only from analysed games, labelled as such
Date: 2026-11-13 · Phase: 4 · Status: Accepted
Context: Only games someone asked Lichess to analyse carry `[%eval]`, a small and non-random subset.
Decision: Compute blunder tables from that subset only, show the subset size, and label it in the app.
Why: Running an engine over millions of games is out of scope. Hiding the bias would be the worse choice.
Label: defensible.
Revisit if: we add a Stockfish pass on a random sample to measure how biased the analysed subset is.

## D-009 — Stream raw files from the Lichess URL; don't keep a local raw copy
Date: 2026-11-13 · Phase: 2 · Status: Accepted
Context: A 2024 month is ~30 GB compressed. Keeping raw files plus bronze would need ~60 GB per big month on a laptop with ~134 GB free. Builds on D-002.
Options: (a) download raw to `data/raw/`, then parse; (b) stream HTTP → zstd → parser → Parquet with nothing raw on disk; (c) store raw/bronze in cloud object storage; (d) query the HF Parquet remotely instead of parsing.
Decision: (b) for every month. A local file path is still accepted for test fixtures and tiny 2013 months. (d) is used only as a cross-check and benchmark comparison, per D-001.
Why: Disk at peak drops to the bronze output only. Lichess monthly files are immutable and permanently hosted, so the source acts as the raw layer: reprocessing means re-streaming, which costs download time instead of disk. Integrity is kept by computing SHA-256 on the stream and comparing it to Lichess's published checksum before the new partition replaces the old one.
Label: defensible. Fragile points: a rerun of a 2024 month re-downloads ~30 GB; a dropped connection mid-stream fails the whole month (no resume), so retries restart from zero.
Revisit if: Lichess changes or removes old files, reruns of big months become frequent, or connection drops make retries too expensive. Then keep raw in Cloudflare R2 (no egress fees, ~$0.015/GB-month past the free 10 GB).

## D-010 — Partition by source file month, not by `UTCDate`
Date: 2026-11-13 · Phase: 0 · Status: Accepted
Context: The 2013-02 file contains 166 games with `UTCDate` 2013-01-31. A month file is not the same set as "games dated that month".
Options: (a) `month` = month of the source file; (b) `month` = month of `UTCDate`, which would make one file write into two partitions.
Decision: (a). `UTCDate` stays a column.
Why: Each writer has to own exactly one partition for atomic overwrite (D-005). Under (b), rerunning 2013-02 would also have to rewrite 2013-01.
Label: defensible. Fragile point: if a game ever appears in two adjacent files, per-month dedup won't catch it. Check once two adjacent months are in silver.
Revisit if: gold needs exact calendar-month stats, in which case filter on `utc_date` in gold instead of moving partitions.

## D-011 — Speed class from `TimeControl`, never from `Event`
Date: 2026-11-13 · Phase: 0 · Status: Accepted
Context: `Event` carries a speed label ("Rated Blitz game"), but Lichess's categories changed over time. In 2013-02, 39,385 of 41,939 "Classical" games are rapid by today's formula (base + 40 × increment).
Options: (a) parse the label from `Event`; (b) compute `tc_bucket` from `TimeControl` with the current formula.
Decision: (b).
Why: One definition across all years, so 2013 and 2024 rating-band × speed cells are comparable. The label would mix today's rapid games into 2013 classical.
Label: defensible.
Revisit if: we want Lichess's historical labels as shown to users at the time, for example to match their own stats pages.

## D-012 — Strict game validation: missing headers and truncation make a game malformed, not null
Date: 2026-11-13 · Phase: 1 · Status: Accepted
Context: The parser has to choose what to do with a game that doesn't look like the samples: a missing header, an unparseable value, or movetext cut off (the last game of a dropped stream).
Options: (a) fill gaps with null and keep the row; (b) reject the game, count it in `ParseStats` by reason, and keep parsing.
Decision: (b). The 13 headers present in 100% of both phase 0 samples are required. Movetext must end in a result token that matches the `Result` header, with balanced `{}`/`()`. Values that don't parse (`WhiteElo "abc"`, a `Site` that isn't `https://lichess.org/<8 alnum>`) also reject the game. The known null forms are the exceptions: `?` Elo/ECO/Opening and absent RatingDiff.
Why: A truncated game has a correct header block, so under (a) it would land in bronze with a wrong `ply_count` and nobody would notice. Requiring the result token is a check that costs nothing, and schema drift shows up as a malformed count instead of a quiet rise in nulls.
Label: defensible. Fragile point: if some untested year lacks one of the 13 headers (e.g. `Termination` in an early file), every game in that month is rejected. The behaviour is loud, which is what we want, but bronze must fail the month on a malformed-rate threshold, not silently write an empty partition (phase 2).
Revisit if: a real month shows a header missing for a legitimate reason; then make that one header optional with an explicit null rule.

## D-013 — Store time of day as a UTC timestamp, not a TIME column
Date: 2026-11-13 · Phase: 2 · Status: Accepted
Context: The `Game` model has `utc_date` and `utc_time`. Parquet has no seconds-unit TIME or TIMESTAMP (PyArrow silently writes `time32[s]` as milliseconds), and Spark 3.5 can't read Parquet TIME columns at all.
Options: (a) `utc_time` as a `"HH:MM:SS"` string; (b) seconds since midnight as an int; (c) `utc_datetime` as `timestamp[ms, UTC]` next to `utc_date`.
Decision: (c). Bronze keeps `utc_date` (date32) and adds `utc_datetime`; there's no separate time column.
Why: Every engine downstream (Spark, DuckDB, pandas) reads a Parquet timestamp natively, so nobody has to re-parse a string or remember what an int means. The date column is redundant, but it's what gold filters on and costs almost nothing in a dictionary-encoded column.
Label: defensible.
Revisit if: we move to Spark 4, which supports TIME, and have a reason to keep time of day without a date.

## D-014 — Bronze guards: 0.1% malformed limit, 2× disk headroom, stdlib HTTP
Date: 2026-11-13 · Phase: 2 · Status: Accepted
Context: D-012 says bronze must fail a month loudly instead of quietly writing a thin partition. The URL stream also needs a free-disk check before a big month starts (CLAUDE.md).
Options: limits of 0 / 0.1% / 1%; disk headroom 1× / 1.5× / 2× the compressed size; `requests`/`httpx` or `urllib`.
Decision: Fail the month if malformed games exceed 0.1% or if no valid games were parsed. Require free disk of 2× the source's Content-Length before streaming. Use `urllib.request`.
Why: 2013-02 has 0 malformed games out of 123,961, so 0.1% (~124 games, or ~90K in a 2024 month) is far above normal and well below what a header change would cause (100% rejected). A limit of 0 would fail a whole 30 GB month over one odd game. Bronze came out 1.42× the compressed size for 2013-02, so 1.5× was too tight and 2× leaves room. `urllib` streams a response body as a file object, which is all we need, without adding a dependency.
Label: defensible. Fragile points: a truncated stream on a small test file (one game cut of 500 = 0.2%) trips the limit, but on a real month the checksum is what catches truncation, not the limit. The 1.42× ratio was measured on 2013 only. 2024 games carry `[%clk]` on every move and may compress differently; re-measure in phase 5.
Revisit if: a real month legitimately exceeds 0.1%, or phase 5 shows bronze is larger than 2× the source for 2024.

## D-015 — No process pool for bronze yet
Date: 2026-11-13 · Phase: 2 · Status: Accepted
Context: PLAN phase 2 task 2 says parallelise inside one file only once single-process speed is measured and is actually the bottleneck.
Measured: 2013-02 bronze end to end is 2.5 s from a local file and 3.3 s from the URL (18.2 MB). Phase 1 put single-core parsing at ~12 MB/s compressed for 2024-style games, so a 30 GB month is ~41 min of parsing, and a 100 Mbit/s download needs ~40 min.
Decision: Single process. The chunked process pool waits for phase 5, where a full 2024 month gives the real split between network time and parse time.
Why: At 100 Mbit/s the download and the parser run at about the same speed, so a pool only helps on a faster link. Adding it before measuring means complexity you can't justify with a number.
Label: defensible.
Revisit if: phase 5 shows parse time clearly above download time for a 2024 month.

## D-016 — `opening_family` = text before the first `:`, minus a trailing `#N`
Date: 2026-11-13 · Phase: 2 · Status: Accepted
Context: PLAN defines the family as the text before the first `:`. In 2013-02 many names have no colon but end in a sibling-line number: `Philidor Defense #3` (1,791 games), `Queen's Pawn Game #2` (1,642), `Pirc Defense #4` (1,004). Under the plain rule each would be its own family.
Options: (a) colon split only; (b) colon split, then strip `\s+#\d+$`; (c) map ECO codes to families with a lookup table.
Decision: (b).
Why: `#N` marks a numbered line inside the same opening, so leaving it in splits one family's sample across several rows and weakens the Wilson bound for no reason. (c) is more precise but needs a curated table to maintain.
Label: defensible. Fragile point: names written with a comma and no colon (`King's Gambit Declined, Queen's Knight Defense`, 280 games; `Englund Gambit Declined, Reversed French`, 302) stay as their own family. Lichess's current naming uses colons there, so check whether 2024 still has comma-only names before deciding whether to handle them.
Revisit if: a 2024 month shows many comma-only names, or gold needs families that match ECO groupings.

## D-017 — Silver: dedup keeps the earliest row; file count sized from row count
Date: 2026-11-13 · Phase: 2 · Status: Superseded in part by D-025 (how dedup and file sizing run; the earliest-row rule stands)
Context: `dropDuplicates` keeps an arbitrary row when duplicates differ, so two runs could keep different rows. Separately, Spark's default 200 shuffle partitions wrote 200 files of ~200 KB for a 124K-row month (AQE doesn't merge them once the frame is cached).
Options: dedup with `dropDuplicates` vs. `row_number()` over `game_id` ordered by `utc_datetime, movetext`. Files: default partitions, `coalesce(1)`, or `repartition(ceil(rows / 500K), game_id)`.
Decision: `row_number()` with a fixed order. `repartition(ceil(rows / 500K), "game_id")`, written as zstd Parquet (Spark's default is snappy; bronze is already zstd).
Why: A fixed order makes the kept row deterministic, which the "reruns give the same aggregates" rule needs once duplicates actually show up (0 in 2013-02). The row count is already computed for the DQ check, so sizing files from it costs nothing. 2013-02 → 1 file of 23.6 MB instead of 200 tiny ones. `coalesce(1)` would put a 2024 month (~90M games) into one task and one huge file.
Label: defensible. The 500K rows per file is a guess until phase 5 measures file size against read time. At 2013's ~190 bytes per silver row that's ~95 MB per file; 2024 rows with clocks are bigger.
Revisit if: phase 5's file-size benchmark points to a different target.

## D-018 — Airflow tasks run the pipeline CLIs in their own venv
Date: 2026-11-14 · Phase: 3 · Status: Accepted
Context: The Airflow image has its own pinned dependency tree (constraints file). The pipeline pins pyarrow, pyspark and zstandard in `uv.lock`.
Options: (a) `pip install` the pipeline deps into Airflow's environment and call `lichess.bronze.run()` from a `@task`; (b) build a second venv from `uv.lock` in the image and have each task run `python -m lichess.bronze ...` as a subprocess.
Decision: (b). Exit codes carry the retry decision: 0 done (JSON on stdout says written or skipped), 3 bad data (fail without retrying), anything else retry.
Why: Tasks run the exact versions the tests ran, and an Airflow upgrade can't silently move pyarrow. The command in the task log is the same command in CLAUDE.md, so any failure reproduces outside Airflow with a copy-paste. The JVM also lives and dies with the subprocess instead of inside Airflow's task process.
Label: defensible. Fragile point: results pass back by parsing the last JSON object on stdout; a library that prints a line starting with `{` after the summary would break that. Both CLIs print the summary last.
Revisit if: we move to KubernetesExecutor or Docker-per-task, where the image is the isolation and the venv is redundant.

## D-019 — A `months` param, not a monthly schedule with catchup
Date: 2026-11-14 · Phase: 3 · Status: Accepted
Context: Airflow's own backfill model ties one DAG run to one data interval (`@monthly`, `catchup=True`, logical date = month).
Options: (a) `@monthly` + catchup; (b) `schedule=None` and a `months` list param, one mapped task group per month.
Decision: (b), `max_active_runs=1`.
Why: The unit of work is a source file, not a time window (D-010), and Lichess publishes each file days after the month ends, so a schedule would fire before the file exists. A list param makes "backfill 6 months" and "rerun one month" the same trigger call, and the grid shows all months of a backfill side by side. One run at a time, because two runs on the same month would race on the partition swap.
Label: defensible. Cost: no automatic monthly runs. Adding a schedule later means a small sensor on `sha256sums.txt` listing the new file.
Revisit if: the project needs to pick up new months unattended.

## D-020 — Skip bronze when the checksum matches; silver always reruns
Date: 2026-11-14 · Phase: 3 · Status: Accepted
Context: PLAN phase 3 asks bronze to skip months already loaded unless `force=true`. Silver depends on bronze.
Options: silver skips with bronze (default trigger rule) vs. silver runs whenever bronze didn't fail (`none_failed`).
Decision: Bronze skips if `_stats.json` says `checksum_verified` and its SHA-256 equals the published one. Silver uses `none_failed`, so it rebuilds even when bronze was skipped. Bronze retries 3 times with exponential backoff (1 min → 30 min max) and `max_active_tis_per_dag=1`; silver retries once, also capped at 1. Exit code 3 fails either task without retrying.
Why: The checksum is the identity of the input, so a match proves bronze is already built from those exact bytes. Silver is minutes against a 30 GB stream, and rerunning it means a hand-deleted or stale silver partition gets fixed by a normal run. One bronze at a time because two streams just split the bandwidth; one Spark job at a time because `local[*]` already uses every core and the Docker VM has 7.75 GB.
Label: defensible. Fragile point: the skip trusts `_stats.json`; someone deleting a part file by hand leaves a "current" partition with missing rows. Silver's DQ check doesn't catch that (it compares to bronze), so run with `force=true` after any manual change.
Revisit if: silver for a 2024 month gets slow enough that rerunning it every backfill hurts; then skip silver too, keyed on the bronze SHA-256.

## D-021 — Local Airflow without login
Date: 2026-11-14 · Phase: 3 · Status: Accepted
Context: The official compose uses FAB auth and creates an `airflow`/`airflow` user.
Decision: SimpleAuthManager with `SIMPLE_AUTH_MANAGER_ALL_ADMINS=true`, UI bound to `127.0.0.1:8080` only. Also dropped from the official file: Redis, Celery worker, Flower, triggerer.
Why: Single-user laptop setup; a shared default password protects nothing that the localhost bind doesn't already. Fewer services also leave more of the 7.75 GB Docker VM for Spark.
Label: defensible for local use only. Never run this compose on a host with a public port.
Revisit if: Airflow ever runs anywhere but this laptop.

## D-022 — Evals as a per-ply centipawn array in silver, mate as a ±10000 sentinel
Date: 2026-11-14 · Phase: 4 · Status: Accepted
Context: The blunder model needs the eval after every ply. PLAN says parse it in Spark, not SQL. Evals come as pawns (`0.12`) or mate (`#3`, `#-2`), and the last ply usually has none (1,278 of 4,195 analysed 2013 games).
Options: (a) a Python UDF that walks the movetext; (b) one `regexp_extract_all` that matches "SAN move + optional `{comment}`", then `transform` each comment to an int; mate as (i) a separate array, (ii) a sentinel value, (iii) clamped already in silver.
Decision: (b) with (ii). `evals_cp array<int>`, one element per ply, null for a ply without an eval, null for the whole column when `has_eval` is false. Mate is +10000 (White mates) or −10000. Silver fails the month if `size(evals_cp) != ply_count` for any analysed game.
Why: Native Spark expressions avoid Python serialization on every row, which matters at 2024's ~8M analysed games per month. The length check compares this regex with the parser's independent ply count, so a misread movetext fails the job instead of shifting every eval one ply. Silver keeps the real centipawn values; how hard to cap is a metric choice, so it belongs in gold.
Label: defensible. Fragile points: a comment before the first move (`{ intro } 1. e4`) would make the regex read words inside it as moves. None seen in either phase 0 sample, and the length check would fail the month if it showed up. The sentinel drops mate distance (`#1` and `#12` look the same), which the 300 cp blunder rule doesn't need.
Revisit if: a later metric needs mate distance, or the length check ever fires.

## D-023 — Blunder rule: 300 cp drop for the mover, evals clamped to ±1000, family grain
Date: 2026-11-14 · Phase: 4 · Status: Accepted
Context: PLAN defines a blunder as an eval swing of ≥300 cp against the side that moved, with mate capped. 2013 H1 has only 4,194 analysed games (69 at 1200–1299) spread over 864 opening lines.
Options: cap at ±1000 / ±2000 / Lichess's winning-chance curve; grain at opening line or at `opening_family`.
Decision: Clamp every eval (mate sentinels included, D-022) to ±1000 cp, then count a blunder when the mover's eval drops by ≥300. Ply 1 and any ply where either side's eval is missing are skipped. Grain: `opening_family × rating_band × tc_bucket × color`, where `color` is the side that blundered. Phases by full-move number: 1–10, 11–25, 26+. Per-game rates divide by every analysed game in the cell.
Why: A ±1000 cap means "lost/won" is one state, so going from +15 to +9 or from mate-in-3 to mate-in-5 isn't a blunder, while throwing away a winning position (+5 → −1) still is. Line grain would put 5 games in most cells, which is noise; families give cells of usable size.
Label: defensible. Fragile points: inside the clamp, +12 → +4 counts as a 600 cp blunder even though the mover is still winning; the winning-chance curve Lichess itself uses doesn't have this problem and is the better rule if the metric ever matters. Dividing by all games makes late-phase rates depend on game length. Also, DuckDB's `greatest()`/`least()` ignore NULLs, so a naive clamp turned a missing final eval into −1000 and added 642 false White blunders. The `clamp` macro keeps NULLs, a dbt unit test pins it, and a Python recount matched (7,434 White, 7,345 Black).
Revisit if: a 2024 month gives enough analysed games for line grain, or the app needs blunders that match Lichess's own `??` marks.

## D-024 — `mart_best_openings`: N = 100 games, ranked per opening name across ECO codes
Date: 2026-11-14 · Phase: 4 · Status: Accepted
Context: PLAN asks for a minimum-games cutoff N chosen from the data. On 2013 H1 (957,657 games after stg filters), White openings per band × speed cell:

| N | cells kept | share of games | band×speed slices left | worst-case Wilson half-width |
|---|---|---|---|---|
| 30 | 5,516 | 80.8% | 40 | ±0.18 |
| 50 | 3,657 | 73.4% | 35 | ±0.14 |
| 100 | 1,907 | 60.7% | 30 | ±0.10 |
| 200 | 896 | 46.0% | 26 | ±0.07 |

Separately, 94 opening names occur with more than one ECO code.
Decision: N = 100, as the dbt var `min_games_best_openings`. The mart rolls `fct_opening_performance` up to `opening` (ECO codes listed in `eco_codes`) and ranks by Wilson lower bound, ties broken by games, then name.
Why: The Wilson bound already handles sample size in the ranking; N exists so the app doesn't show lines whose interval is too wide to mean anything. ±0.10 is roughly the gap between a good and a bad opening at club level. At 1200 blitz it still leaves 26 White lines to rank. Users pick an opening by name, so splitting one name over two ECO rows would split its sample.
Label: defensible. Fragile point: N is tuned to 6 months of 2013. A 2024 month has about 100× the games, so N = 100 would let thousands of thin lines in. Re-pick it (or tie it to slice size) once phase 5 loads 2024.
Revisit if: phase 5 adds 2024 months, or the app wants family-level rankings too.

## D-025 — Silver dedup shuffles only `game_id`; checks run on staged output, no cache
Date: 2026-11-18 · Phase: 5 · Status: Accepted
Context: Before streaming a 2024 month, silver ran on the first 100 MB of 2024-09 (308K games). Spark's temp dir peaked at 547 MB, 5.5x the compressed input, because the dedup window and `repartition(files, "game_id")` each shuffled every column, and `movetext` with `[%clk]` on every move is 92% of the bytes. Scaled to the 28.6 GB month: ~156 GB of shuffle files on top of ~41 GB bronze and ~42 GB silver, against 129 GB free. Dropping `.cache()` alone made it worse (798 MB): the cache wasn't the problem, the shuffles were.
Options: (a) zstd shuffle codec (measured 336 MB, still ~96 GB for the month); (b) one shuffle instead of two (estimated ~half); (c) find duplicate ids from the id column only, broadcast that list, pass unique rows through with no shuffle, window only the duplicated rows.
Decision: (c). File sizing uses `coalesce(ceil(rows / 500K))`, which merges input splits without a shuffle. No `.cache()`: the month is written to `data/silver/_staging_games/`, the DQ checks run on the written files, then the partition is swapped in with the same two-rename swap as bronze. Dynamic partition overwrite is gone.
Why: Measured on the same sample: Spark temp 547 MB → 10.6 MB (0.1x input), runtime 11.7 s → 9.5 s. 2013 H1 silver and every gold table came out identical (row counts, fingerprints, mart ranking hash). Checking the written files is also stricter than checking a cached frame, and a failed check still leaves the old partition untouched.
Label: defensible. Fragile points: the broadcast assumes few duplicates; a month where millions of ids repeat (a botched Lichess re-export) would broadcast a big list, and the driver's 4 GB is the limit. `coalesce` can only merge splits, never split them, so a silver file is never smaller than one bronze input split and can't be tuned below that. A rerun briefly holds the old and new partition at once (the swap), so it needs one extra month of silver free.
Revisit if: a month shows a real duplicate rate, or phase 5's file-size benchmark wants files that differ from the bronze file layout.

## D-026 — Silver drops `movetext`; bronze Parquet is deleted after silver for months ≥ 2017-01
Date: 2026-11-18 · Phase: 5 · Status: Accepted
Context: Goal for phase 5 is the least local disk that still gives the same app. On the 2024-09 sample, `movetext` is 92% of bronze and 90% of silver, and no gold model reads it (gold reads `evals_cp`, which silver already extracts, D-022). Even after D-025, one 2024 month projected to ~41 GB bronze + ~42 GB silver kept on disk, so 1 month fit and 2 didn't.
Options: (1) drop `movetext` from silver; (2) also delete bronze once silver passes; (3) strip `[%clk]` from bronze movetext; (4) keep a 10% hash sample of 2024; (5) have the parser write slim rows and skip full bronze.
Decision: (1) + (2). Silver keeps every other column. `silver_games --drop-bronze` deletes the month's bronze Parquet after the swap and keeps bronze's `_stats.json` with `parquet_dropped: true`. Silver writes its own `_stats.json` with the `bronze_sha256` it was built from. With bronze dropped, silver skips if its stats match that SHA-256 and fails with "re-stream" otherwise. The DAG passes `--drop-bronze` for months ≥ the `drop_bronze_from` param (default `2017-01`). Silver files target 2.5M rows (~115 MB at 2024 row size) instead of 500K, since rows are ~10x smaller.
Why: Sample: silver 146 → 14.3 MB. Projected 2024 month: ~4 GB kept instead of ~83 GB, with a ~48 GB peak while it runs (bronze plus silver plus ~3 GB Spark temp), so the disk caps the peak, not the number of months. 2013 H1 silver and all gold tables came out identical. Keeping the bronze stats keeps the verified checksum and parse counts on record, and bronze's skip-if-current works unchanged. (3) breaks the raw-bronze promise for a smaller gain; (4) weakens the full-scale claim; (5) reverses D-022 and leaves the Spark job with nothing to do.
Label: defensible. Costs and fragile points: any silver change for a dropped month (a new column, a bug fix) means re-streaming ~30 GB per month at ~45 min each, so 2013–2016 keep bronze for cheap iteration. Deleting a silver partition by hand after bronze was dropped makes that month fail until a `force` run. `movetext`-level analysis (openings by move sequence, clock use) needs bronze, which then has to be rebuilt.
Revisit if: gold needs anything from movetext beyond evals, or disk stops being the constraint.

## D-027 — Process-pool parser built and measured, not wired into bronze
Date: 2026-11-18 · Phase: 5 · Status: Accepted
Context: PLAN phase 2 task 2 says to parallelise parsing inside one file only once single-process speed is measured and is the bottleneck. Phase 5 task 2 asks for single vs. pool games/sec.
Options: (a) wire the pool into bronze; (b) keep bronze single-process and keep the pool as a measured, tested option.
Decision: (b). `lichess.parallel` (one reader splits at `[Event ` lines into 20K-game chunks, at most 2× workers chunks in flight) is tested to give the same games and stats as the single-process parser, and benchmarked in `benchmarks/parse_parallel.py`. Bronze doesn't use it.
Why: On the 2024 sample the pool reaches 1.5× with one writer (2.3× if workers wrote their own output), capped by the serial parent: decompress, chunk, pickle. The real 2024-09 stream arrives at ~2.1 MB/s, while the single-process parser handles 11.3 MB/s, so parsing isn't the bottleneck and the pool would add processes, pickling and a second code path to buy nothing.
Label: defensible: measured first, didn't build the complex thing into the pipeline. Fragile point: the parser logs a malformed game's line number relative to its chunk; `ParseStats` examples are made absolute, but the log lines aren't.
Revisit if: months are loaded from a local copy or over a ≥100 Mbit/s link. Then have each worker write its own Parquet files, since the measured ceiling says shipping `Game` objects back to one writer wastes most of the gain.

## D-028 — No skew handling in the pipeline; small lookups are broadcast
Date: 2026-11-18 · Phase: 5 · Status: Accepted
Context: `opening_family` is skewed (Sicilian Defense is 10.3% of 2013 H1 rows). PLAN phase 5 task 5 asks for before/after numbers on a skewed group-by and join.
Options: (a) turn on AQE skew join with lowered thresholds in `spark_session`; (b) salt joins on `opening_family`; (c) nothing, plus broadcast for any join to a small lookup.
Decision: (c). `benchmarks/skew.py` keeps the measured AQE and salting variants. Pipeline config is unchanged.
Why: On 19.3M rows the group-by isn't skewed: partial aggregation sends 1,467 records through the shuffle. Every gold model is that shape, and silver's only window is on `game_id`, which is unique per row. The skewed join is 2.43 s as a sort-merge join and 1.38 s with the 287-row lookup broadcast. AQE skew join (longest task 1,198 → 524 ms) and salting (→ 266 ms) didn't change wall time on 10 cores, and salting cost 2.3×.
Label: defensible. Fragile points:
- AQE skew join stays silent below its 256 MB / 64 MB defaults.
- It also stays silent when one side is already partitioned on the join key (e.g. a cached group-by), which was measured here.
- So "AQE is on" is not evidence that skew is handled. Check the task count in the UI.
Revisit if: the pipeline gains a join between two large tables on a skewed key, a window partitioned by a skewed key, or runs on a cluster with many more cores than 10, where the straggler becomes the floor.

## D-029 — Drop result `*` games in `stg_games`, not in silver
Date: 2026-11-18 · Phase: 5 · Status: Accepted
Context: The first dbt build with 2024-09 failed `not_null_stg_games_white_score` on 20,430 rows. The full month has a `Termination` value the 2013 data and the 100 MB 2024 sample didn't: `Unterminated`. These are 23,054 correspondence games still in progress when the month was dumped, with Result `*`. Silver's `is_decisive_play` only excludes `Abandoned` and games under 2 moves, so 20,430 of them passed it with a null `white_score`.
Options: (a) fix `is_decisive_play` in silver to also require a result, then rebuild 2024-09 silver; (b) add `white_score is not null` to the `stg_games` filter.
Decision: (b). Silver keeps the rows and its flag as is. Gold now requires a score.
Why: (a) means re-streaming the month (5 h at the measured link), because its bronze was dropped (D-026). (b) gives the same gold for every month at no cost. It changes nothing for 2013 (0 games with result `*`), and the 2024-09 side-game count matches 2 × (87,242,165 − 20,430) exactly. The dbt test did its job: it caught a value nobody had seen in the samples.
Label: defensible. Fragile point: silver's `is_decisive_play` now doesn't mean "counts for stats" on its own. Anything reading silver directly (the app doesn't) must also require a score. When silver is next rebuilt for another reason, fold the condition into the flag.
Revisit if: silver gets rebuilt anyway, or another consumer reads silver directly.

## D-030 — Bronze resumes a dropped stream with HTTP Range requests
Date: 2026-11-19 · Phase: 5 · Status: Accepted
Context: The first 2024-08 run died 63 minutes in. A 60 s read timeout fired after the link stalled, and the month had to restart from byte 0. At 30 GB per month and 1–5 MB/s, one stall can cost hours, and a link change always kills the connection.
Options: (a) leave it to retries (Airflow / rerun the month from byte 0); (b) on a read error or early EOF, reopen at the next byte with `Range: bytes=N-` and keep feeding the same SHA-256 hasher and zstd decompressor; (c) download in parallel ranges to a file.
Decision: (b). `ResumingReader` resumes with exponential backoff capped at 60 s, and gives up after 30 resumes in a row that got no new bytes (about a 30 minute outage). Drops with progress in between don't count. The first version capped total resumes at 50, and 2024-10 hit that at byte 14.45 GB of 30.74 GB after 22 h: one drop every 20–60 min, each after new data. 2024-08 finished on the same link after 24 resumes. A resume must get HTTP 206 with a `Content-Range` starting at exactly N, or the month fails at once.
Why: Bytes still arrive contiguous and exactly once, so D-009 holds: the checksum covers the whole file, and nothing raw touches disk. Tests show a stream cut 5 times (by exceptions or early EOFs) gives byte-identical Parquet. Lichess answered a real resume with 206 `bytes 1000000-…`. (c) breaks the no-raw-on-disk rule and needs 30 GB of scratch space.
Label: defensible. Fragile points:
- It trusts the file not to change between resumes. Lichess files are immutable, and the end-of-stream checksum is the backstop, but a mismatch is only caught at the end.
- A server or proxy that ignores `Range` is refused rather than spliced in.
Revisit if: streams need to resume across process restarts. That needs checkpointed parser and writer state, which this doesn't keep.

## D-031 — Score blunders with a list lambda per game, not unnest + join
Date: 2026-11-22 · Phase: 5 · Status: Accepted
Context: After three 2024 months, `dbt build` went from 71 s to 562 s. 537 s of that was `fct_opening_blunders`, which took 584 s when run alone. Its input is small: 25.0M analysed games and 1.64B evals, scanned in 2.9 s. DuckDB's JSON profile showed where the time went:
- the `from analysed, unnest(generate_series(2, len(evals_cp)))` lateral join is decorrelated into a hash group-by of the games, eval lists included (907 CPU-s);
- then a hash join of 1.6B plies back to them (1,439 CPU-s, 580 GB intermediate);
- then a left join of the blunders back to every game × colour (208 CPU-s);
- peak memory 13.8 GB against a 12.7 GB limit, and 30.7 GB spilled to temp disk.
Options: (a) raise `memory_limit` / threads; (b) keep one row per ply, but carry the grouping columns through the unnest so no join back is needed; (c) score each game's `evals_cp` in place with `list_filter(range(2, n + 1), lambda p: ...)`, giving the list of blunder plies, then split by colour and phase with `len(list_filter(...))`.
Decision: (c). No unnest, no join, one pass over the games.
Why: It is the same rule written over the list instead of over rows. On three 2024 months it takes 26.4 s alone (22× faster) and 42 s inside `dbt build`, which now takes 70 s. The output is identical to the old table (`EXCEPT ALL` both ways: 0 rows), and the hand-built unit test still passes. (a) treats the symptom on a 16 GB laptop. (b) still materializes 1.6B rows.
Label: defensible. Fragile points:
- The lambda reads `evals_cp` from the enclosing row, which needs DuckDB's lambda column capture (present in 2.0.0).
- `blunders` is now cast to `BIGINT`. The old HUGEINT sum exported to Parquet as `DOUBLE`.
Revisit if: the blunder rule needs per-ply context the list form can't express cleanly (e.g. the move played). Then use (b).

## D-032 — Demo reads two Parquet files plus a JSON sidecar, loaded whole into pandas
Date: 2026-11-22 · Phase: 6 · Status: Accepted
Context: PLAN says export the gold tables to "one small Parquet file". There are two tables with different grains (opening × band × speed × colour, and opening family × band × speed × colour), plus the app needs the months covered and the sample size.
Options: (a) one Parquet with a `table` discriminator column and nullable columns from both; (b) ship `gold.duckdb` itself; (c) one Parquet per table plus `meta.json`.
Decision: (c). `best_openings.parquet` (152,572 rows, 4.26 MB), `blunders.parquet` (31,814 rows, 0.65 MB), `meta.json`. The app reads them with pandas and filters in memory; no DuckDB in the Space.
Why: (a) is a union of two unrelated schemas that every reader has to un-union. (b) ties the Space to the exact DuckDB storage version and copies the 270M-row `stg_games` view definition that points at local silver. At 5 MB, loading everything into pandas once and filtering per request is simpler than a query engine and keeps the Space's dependencies to streamlit alone.
Label: defensible.
Revisit if: the export grows past ~100 MB (more months or finer grain), at which point pushing filters into Parquet row groups with DuckDB or pyarrow beats loading it all.
