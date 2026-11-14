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
Date: 2026-11-13 · Phase: 2 · Status: Accepted
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
