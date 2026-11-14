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
Date: 2026-11-13 · Phase: 1 · Status: Proposed (confirm with phase 1 throughput numbers)
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
