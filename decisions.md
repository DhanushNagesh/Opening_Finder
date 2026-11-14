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
