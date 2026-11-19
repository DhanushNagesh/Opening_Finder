# Benchmark results

Real measurements only. Each row says what was run, on what, and when.

## Parser throughput, one core (phase 1)

Machine: Apple M4, macOS 15 (Darwin 24.6), Python 3.11.16, zstandard 0.25.0, python-chess 1.11.2.
Date: 2026-11-13. Script: `benchmarks/parse_throughput.py`, one mode per process, single run each.

| Input | Mode | Games | Seconds | Games/sec | Compressed MB/s | Peak RSS |
|---|---|---|---|---|---|---|
| 2013-02 full file (18.2 MB .zst) | decompress + split lines only | 123,961 | 0.21 | 592,733 | 86 | 37 MB |
| 2013-02 full file | **our parser** | 123,961 | 2.02 | **61,260** | 9.0 | 33 MB |
| 2013-02 first 10,000 games | our parser | 10,000 | 0.17 | 60,558 | – | 32 MB |
| 2013-02 first 10,000 games | python-chess `read_game` | 10,000 | 5.24 | 1,908 | – | 47 MB |
| 2024-01 first 50 MB of .zst (stdin, in memory) | decompress + split lines only | 155,057 | 0.52 | 297,480 | 96 | 93 MB* |
| 2024-01 first 50 MB of .zst | **our parser** | 155,056 ok + 1 malformed | 4.14 | **37,437** | 12.1 | 90 MB* |

\* includes the 50 MB input buffered in memory so network time stays out of the measurement. On a file, RSS stays flat at ~33 MB for the whole month; it doesn't grow with file size.

The 2024 sample's one malformed game is the last one, cut mid-header by the byte range. The parser counted it (`bad_header_line`) and didn't crash.

What the numbers say:
- **Our parser is ~32× faster than python-chess** (60.6K vs 1.9K games/sec on the same 10K games), mostly because python-chess replays every move on a board. That confirms D-003.
- **Parsing, not decompression, is the bottleneck.** zstd plus line splitting takes ~10% of the parse time in 2013 and ~13% in 2024.
- **Bytes per game, not games, drive the cost.** 2024 games carry `[%clk]` on almost every move, so games/sec drops from 61K to 37K, while compressed MB/s rises (9 → 12) because there's more comment text to strip with a regex per game.

Projection, not a measurement: a full 2024 month (~30 GB compressed) at 12.1 MB/s is ~41 minutes of single-core parsing. Streaming it from the URL needs a ~100 Mbit/s download to keep up. Phase 2 measures the real end-to-end number.

## Bronze and silver, one month end to end (phase 2)

Same machine as above. Date: 2026-11-13. Single runs; Spark is `local[*]` on a 10-core M4 with 4 GB driver memory, JDK 17.

| Stage | Input | Rows | Seconds | Output |
|---|---|---|---|---|
| bronze, local file | 2013-02 .zst, 18.2 MB | 123,961 | 2.45 | 1 file, 25.7 MB (zstd Parquet) |
| bronze, streamed from URL | 2013-02 .zst, 18.2 MB | 123,961 | 3.31–3.36 (two runs) | same; byte-identical across runs |
| silver (Spark) | bronze 2013-02 | 123,961 | 5.05 | 1 file, 23.6 MB (zstd Parquet) |

Bronze from the URL adds ~0.9 s of network time to ~2.5 s of parsing for this month, so the parser is still the larger cost at 2013 sizes. Silver time is mostly JVM start-up and fixed Spark overhead at this size; it's not a throughput number. Phase 5 measures a 2024 month.

## Silver temp disk on a 2024 sample (phase 5, before scaling)

Same machine. Date: 2026-11-18. Input: first 100 MB of `lichess_db_standard_rated_2024-09.pgn.zst` (`curl -r 0-99999999`), 308,422 games, bronze 143.8 MB in 2 files. Spark `local[*]`, 4 GB driver. Peak = largest `du -sk` of `SPARK_LOCAL_DIRS`, sampled every 0.1 s while the job ran. Single runs (the new version twice).

| Silver version | Peak Spark temp | × compressed input | Runtime | Silver output |
|---|---|---|---|---|
| Window dedup + `repartition` + `.cache()` (D-017) | 547 MB | 5.5× | 11.7 s | 146 MB |
| same, `.cache()` removed | 798 MB | 8.0× | 9.3 s | 146 MB |
| same, `spark.io.compression.codec=zstd` | 336 MB | 3.4× | 12.2 s | 146 MB |
| **id-only dedup + coalesce + staged checks (D-025)** | **10.6 MB** | **0.1×** | **8.9–9.5 s** | 146 MB |

Where the bytes are (Parquet column chunks, zstd): `movetext` is 92% of bronze (131.7 of 143.8 MB) and 90% of silver. Silver without `movetext` is 14.3 MB. Stripping clock-only comments from `movetext` takes it from 131.7 to 77.7 MB.

The old job shuffled every column twice, and lz4 shuffle files of clock-heavy movetext are several times the zstd input. Scaled linearly to the full 28.6 GB month, the old job needed ~156 GB of temp next to ~41 GB bronze and ~42 GB silver, more than the 129 GB free. The new one needs ~3 GB of temp, so the month fits at a projected ~86 GB peak. That's a projection from the sample; the full-month run in this phase measures the real number.

## Scale: 2013-02 to one full 2024 month (phase 5, task 1)

Same machine (Apple M4, 10 cores, 16 GB, ~460 GB SSD). Date: 2026-11-18. Commands: `uv run python -m lichess.bronze --month 2024-09` (streamed from the Lichess URL, SHA-256 checked as the bytes pass), then `uv run python -m lichess.spark_jobs.silver_games --month 2024-09 --drop-bronze`. Each stage was wrapped in `/usr/bin/time -l`. Disk was sampled every 5 s: `df` free space, and `du` of the bronze staging, bronze and silver dirs. Single run. The parser, engine, file-size and skew benchmarks ran on the same machine during the stream, but not during silver.

| Stage | Month | Input | Rows | Wall time | CPU | Output |
|---|---|---|---|---|---|---|
| bronze, from URL | 2013-02 | 18.2 MB .zst | 123,961 | 3.3 s | — | 25.7 MB, 1 file |
| silver | 2013-02 | bronze | 123,961 | 5.1 s | — | 23.6 MB, 1 file (with movetext, phase 2) |
| **bronze, from URL** | **2024-09** | **28.57 GB .zst** | **87,713,219** | **18,484 s (5 h 8 min)** | 4,640 s user, peak RSS 716 MB | **41.3 GB, 351 files** |
| **silver + drop bronze** | **2024-09** | 41.1 GB bronze | 87,713,219 | **577 s (9.6 min)** | (in the Spark JVM) | **4.24 GB, 36 files (~118 MB each)**; bronze Parquet deleted, `_stats.json` kept |

2024-09 checks:
- Checksum verified.
- Data quality: 0 malformed games, 0 stray lines, 0 null ids, 0 duplicates.
- `count(distinct game_id)` = rows = 87,713,219.
- tc_bucket split: blitz 41.55M, bullet 32.34M, rapid 12.47M, ultrabullet 0.78M, classical 0.49M, correspondence 0.09M.

| Disk, 2024-09 | GB |
|---|---|
| Free before | 125.7 |
| Peak in pipeline dirs (bronze 41.3 + silver staging, just before the swap) | **47.1** |
| Peak drop in free space (also counts Spark temp and anything else on the machine) | 52.5 |
| Kept after the run (silver only) | **4.24** |

Takeaway:
- **One 2024 month is 708× the rows of 2013-02.** It ran on a laptop with a 47 GB peak and 4.2 GB kept. That matches the D-026 projection of ~48 GB peak and ~4 GB kept, from the 100 MB sample. Before D-025/D-026, the projection was ~156 GB of Spark temp alone, which wouldn't have fit.
- **The bottleneck is the network, by about 4×.** The stream averaged 1.55 MB/s. The parser used 4,640 CPU-seconds over 18,484 s of wall time, about 25% of one core. That's why the process pool stays unwired (D-027).
- **Silver is cheap by comparison:** 152K rows/s, including the dedup scan, the data-quality checks on the written output, and the bronze delete.
- **How many 2024 months fit: disk is no longer the limit.** Each month needs ~47 GB transient and keeps ~4.2 GB. With ~117 GB free after this run, more than ten months fit before the peak no longer does. The real limits are:
  - time: about 5 h per month at this link speed;
  - PLAN's 100 GB streaming cap, which needs a yes before going past it. 2024-09 used 28.6 GB, so two more months would bring the total to ~90 GB.

## Parser: single process vs. chunked process pool (phase 5, task 2)

Same machine (Apple M4, 10 cores, 16 GB). Date: 2026-11-18. Script: `benchmarks/parse_parallel.py <file> --workers 1 2 4 8`, best of 2 runs per cell, input read from local disk. The 2024-09 bronze stream was running at the same time, using 18–29% of one core (it's waiting on the network), so these numbers may be a few percent low.

The parent process decompresses, splits lines and cuts chunks of 20K games at `[Event ` lines (`src/lichess/parallel.py`). Workers run the same `PgnParser`. `pool-games` sends `Game` objects back to the parent, which is what bronze's single Parquet writer would need. `pool-count` returns only counts, the ceiling if each worker wrote its own Parquet.

| Input | Mode | Workers | Games/sec | Compressed MB/s | vs. single |
|---|---|---|---|---|---|
| 2013-02 full (18.2 MB, 123,961 games) | single | 1 | 59,687 | 8.7 | 1.0× |
| | pool-games | 1 / 2 / 4 / 8 | 42,566 / 72,466 / 99,025 / 103,801 | 6.2 / 10.6 / 14.5 / 15.2 | 0.7 / 1.2 / 1.7 / 1.7× |
| | pool-count | 1 / 2 / 4 / 8 | 53,728 / 100,271 / 131,902 / 142,266 | 7.9 / 14.7 / 19.3 / 20.8 | 0.9 / 1.7 / 2.2 / 2.4× |
| 2024-09 first 100 MB (308,422 games) | single | 1 | 34,811 | 11.3 | 1.0× |
| | pool-games | 1 / 2 / 4 / 8 | 25,368 / 43,698 / 51,139 / 46,827 | 8.2 / 14.2 / 16.6 / 15.2 | 0.7 / 1.3 / 1.5 / 1.3× |
| | pool-count | 1 / 2 / 4 / 8 | 32,055 / 54,751 / 76,244 / 81,102 | 10.4 / 17.8 / 24.7 / 26.3 | 0.9 / 1.6 / 2.2 / 2.3× |

Where the parent's time goes on the 2024 sample, timed separately: decompress + split + chunk 1.54 s (65 MB/s), pickling chunk text to send 0.96 s, unpickling the returned `Game` lists 1.36 s (2,170 pickled bytes per game).

Takeaway: the pool tops out at 4 workers, at 1.5× when `Game` objects come back to one writer and 2.3× when they don't. The ceiling is the single parent: decompressing, chunking and pickling run serially in one process, and shipping parsed games back costs nearly as much as parsing them did. For loading months it doesn't matter. The same 2024-09 file streams from database.lichess.org at ~2.1 MB/s on this connection, so even the single-process parser (11.3 MB/s) is idle ~80% of the time. Bronze stays single-process (D-027). The pool would pay off only with a local copy of the file or a ≥100 Mbit/s link, and then only if each worker wrote its own Parquet files.

## Engine: gold aggregation in DuckDB vs. Spark local[*] (phase 5, task 3)

Same machine (Apple M4, 10 cores, 16 GB). Date: 2026-11-18. Script: `benchmarks/engine_compare.py --engine {duckdb,spark} --months ... --repeat 5`, one engine per process. The query is `fct_opening_performance` (with `stg_games`' filter and the Wilson macro inlined), the same SQL text in both engines. DuckDB materialises it into a temp table and Spark writes it to the `noop` sink, so neither ships rows to Python. DuckDB 2.0.0 with 10 threads; Spark 3.5.9 `local[*]` (10 cores), 4 GB driver, AQE on. The 2024-09 bronze stream was running at the same time (about a quarter of one core).

Every run's result fingerprint (cells, side-games, wins, draws, Σ Wilson bound) was identical across both engines, and at 6 months it matched dbt's `fct_opening_performance`: 83,788 cells, 1,915,314 side-games.

| Silver input | Engine | Engine start | First query | Best warm query (of 4) | DuckDB faster by (warm) |
|---|---|---|---|---|---|
| 2013-02: 123,961 rows, 4.2 MB | DuckDB | 0.02–0.06 s | 0.053 s | 0.026 s | — |
| | Spark, 200 shuffle partitions (default) | 1.53 s | 1.41 s | 0.265 s | 10× |
| | Spark, 10 shuffle partitions | 1.31 s | 1.36 s | 0.218 s | 8× |
| 2013 H1: 966,028 rows, 32.8 MB | DuckDB | 0.01 s | 0.062 s | 0.060 s | — |
| | Spark, 200 shuffle partitions (default) | 1.17 s | 3.31 s | 0.668 s | 11× |
| | Spark, 10 shuffle partitions | 1.06 s | 2.43 s | 0.468 s | 8× |
| 2024-09: 87,713,219 rows, 4,238 MB | DuckDB | 0.01 s | 3.74 s | 4.33 s (3.92 s in an earlier run) | — |
| | Spark, 200 shuffle partitions (default) | 1.33 s | 22.48 s | 19.93 s (18.22 s earlier) | 4.6× |
| 2013 H1 + 2024-09: 88,679,247 rows, 4,271 MB | DuckDB | 0.05 s | 4.96 s | 5.56 s (4.02 s earlier) | — |
| | Spark, 200 shuffle partitions (default) | 1.49 s | 27.17 s | 23.25 s (22.84 s earlier) | 4.2× |

2024 runs (2026-11-18, after the stream had finished, so nothing else was running):
- The query now also requires `white_score is not null`, matching `stg_games` after D-029. 2013 fingerprints are unchanged by that.
- At 7 months, both engines' fingerprints match dbt's `fct_opening_performance` exactly: 433,258 cells, 176,358,784 side-games, 84,791,449 wins.
- Warm times varied by up to ±20% between the two runs, so the earlier run's numbers are shown in brackets. DuckDB's warm runs aren't faster than its first: the input stays in the OS page cache, and each run rebuilds a ~400K-row temp table.
- The full `dbt build` (3 models, 47 tests, 1 unit test) took 71 s with 2024-09 included, vs. ~5 s on 2013 alone.

Takeaway so far: at 2013 sizes DuckDB wins 8–11× on a warm query and about 40× on what a dbt run actually pays (start + first query: 0.08 s vs. 2.9–4.5 s). Spark's cost here is fixed per-query overhead (planning, task scheduling, the shuffle stage), not throughput. Going from 1 to 6 months, 8× the rows, Spark's warm time only grows 2.1–2.5× while DuckDB's grows 2.3×, so neither is anywhere near its limit. Tuning shuffle partitions to the core count trims Spark by 18–30% and doesn't change the verdict. Takeaway at 2024 scale:
- **DuckDB still wins, but by 4–5× instead of 8–11×.** From 2013 H1 to 2024-09 (91× the rows), DuckDB's warm time grew ~70× (0.06 → 3.9–4.3 s) and Spark's ~28× (0.67 → 18–20 s).
- Spark's fixed overhead got diluted, which is the expected curve. The crossover isn't on one machine: 88M rows, 4.2 GB, is one 2024 month, and DuckDB answers it in ~4 s with no JVM.
- **Defensible: keep gold on dbt-duckdb (D-006).** Spark earns its place on a cluster, when silver is many months bigger than one machine's RAM and disk. That's not where this project's gold runs.

## File sizing: many tiny Parquet files vs. a few large ones (phase 5, task 4)

Same machine (Apple M4, 10 cores, 16 GB). Date: 2026-11-18. Script: `benchmarks/file_sizing.py --source data/bronze/month=2013-0{1..6} --files 5000 1000 100 10 1 --repeat 3`. Input: 2013 H1 bronze, 966,028 rows, 201 MB in 6 files. Bronze is used instead of silver because it keeps `movetext`, which makes it big enough (180 MB) to fill one ~128–256 MB file. Silver H1 is only 33 MB.

Method:
- Spark 3.5.9 (`local[*]`, zstd, default 128 MB Parquet block size) writes the cached rows once per layout with `repartition(n)`. Every layout pays the same shuffle and gets exactly n files.
- Each layout is read back by Spark and DuckDB 2.0.0 with two queries:
  - narrow: `group by eco`, `count(*)`, `sum(ply_count)`.
  - full: `sum(length(movetext))` plus two counts, so every byte of the widest column is decoded.
- Each read builds a fresh DataFrame / `read_parquet`, so listing files and reading footers are inside the timing. The table shows the best of 3.
- Results were identical in every layout and in both engines (966,028 rows, 401,187,084 movetext chars).
- The OS page cache was warm for all layouts, so this measures per-file overhead, not disk speed.
- The 2024-09 bronze stream was running at the same time.

| Files | Median file | Total on disk | Spark write | Spark read tasks | Spark narrow | Spark full | DuckDB narrow | DuckDB full |
|---|---|---|---|---|---|---|---|---|
| 5,000 | 52 KB | 259.8 MB (+44%) | 30.59 s | 157 | 3.089 s | 1.409 s | 0.291 s | 0.347 s |
| 1,000 | 211 KB | 211.3 MB (+17%) | 7.68 s | 33 | 0.680 s | 0.746 s | 0.069 s | 0.145 s |
| 100 | 1.9 MB | 191.9 MB (+7%) | 3.04 s | 10 | 0.214 s | 0.419 s | 0.021 s | 0.124 s |
| 10 | 18.5 MB | 185.5 MB (+3%) | 2.48 s | 10 | 0.285 s | 0.378 s | 0.014 s | 0.103 s |
| 1 | 180 MB, 2 row groups | 180.0 MB | 4.72 s | 10 (2 do work) | 0.368 s | 1.659 s | 0.018 s | 0.352 s |

### Rerun on real 2024-09 silver

Date: 2026-11-18, after the stream had finished. Input: `data/silver/games/month=2024-09`, 87,713,219 rows, 4,238 MB in 36 files (~118 MB, one row group each). Command: `benchmarks/file_sizing.py --source data/silver/games/month=2024-09 --files 5000 500 36 10 --repeat 3 --no-cache --method split`.

How this run differs from the 2013 one:
- **Rows are re-read for each layout instead of cached** (`--no-cache`). Caching the full-width month ran the 4 GB Spark driver out of heap, so write times include reading the source, the same for every layout.
- **No shuffle** (`--method split`). The 2013 method's round-robin `repartition(5000)` also ran out of heap: Spark sorts every row before a round-robin repartition. Plain full-width reads and `repartition(36)` passed. The split method coalesces to at most n tasks and caps rows per file at rows/n, so file counts come out near n rather than exactly n.
- Silver has no `movetext`, so the full query reads `opening`, the widest string column left.

Results matched in every layout and both engines.

| Files | Median file | Total on disk | Spark write | Spark read tasks | Spark narrow | Spark full | DuckDB narrow | DuckDB full |
|---|---|---|---|---|---|---|---|---|
| 5,020 | 0.88 MB | 4,410.7 MB (+4%) | 154.6 s | 186 | 5.386 s | 4.772 s | 0.430 s | 0.882 s |
| 522 | 8.2 MB | 4,110.0 MB (−3%) | 114.6 s | 48 | 1.839 s | 3.362 s | 0.170 s | 0.594 s |
| 63 | 109 MB | 4,235.8 MB | 84.6 s | 38 | 1.613 s | 3.353 s | 0.147 s | 0.566 s |
| 16 | 363 MB, ~3 row groups each | 4,238.0 MB | 77.3 s | 34 | 1.483 s | 3.288 s | 0.169 s | 0.593 s |

2024 takeaway:
- **The same shape at 22× the bytes.** Sub-MB files cost 2× on write, 3.3–3.6× on Spark's narrow query, 2.5× on DuckDB's narrow query, and 1.4–1.6× on full scans.
- **From ~8 MB up the curve is flat** (8 MB → 363 MB within ~10%). Big files don't hurt here, because Spark rolls a new row group every 128 MB, so a 363 MB file still gives 3 units of parallelism. The 2013 single-file case was slow only because it had 2 row groups for 10 cores.
- The 522-file layout came out 3% smaller on disk than the larger-file layouts. I haven't checked why.
- **What a month of silver needs:** current silver (36 files, ~118 MB, one row group each) sits on the flat part. It's one of the two cheapest layouts to write, and within noise of the best on reads.
- **New fragile point:** wide full-row Spark work over a whole 2024 month (cache, round-robin repartition) fails at the 4 GB driver heap set in `spark_session`. Nothing in the pipeline does that today: silver reads bronze, and gold is DuckDB.

2013 takeaway:
- Tiny files are expensive on every axis. At 5,000 files of 52 KB:
  - Spark's write is 12× slower than at 10 files (one task and one file commit per file).
  - The same rows take 44% more disk, because every file repeats its footer, schema and dictionaries.
  - Spark's narrow query is 11× slower and DuckDB's is 21× slower, because opening a file and reading its footer costs more than the actual work.
- Spark hides some of this: `openCostInBytes` (4 MB) packs small files into fewer read tasks (5,000 files → 157 tasks). The per-file open cost stays.
- The other extreme also loses, for a less obvious reason: parallelism comes from row groups, not files. The single 180 MB file has 2 row groups, because Spark's default Parquet block is 128 MB. So only 2 of 10 cores decode the wide column. Its full scan is 4.4× slower in Spark and 3.4× slower in DuckDB than the 10-file layout. Its write is single-task, about 2× slower.
- The sweet spot is enough files or row groups to keep every core busy, each well above ~1 MB. At this size that's 10–100 files.
- At 2024 scale this is silver's current rule: `ROWS_PER_FILE = 2.5M` gives ~115 MB files, one row group each, about 35 files per month (D-026). That's ≥3 row groups per core, with per-file overhead well under 1%.
- Defensible: Spark's default after a shuffle is 200 files per month (`spark.sql.shuffle.partitions`). For a 2013 month that's 200 files of ~20 KB, the top row of this table. That's why silver sizes its output explicitly instead.

## Skew: group-by, window and join on `opening_family` (phase 5, task 5)

Same machine (Apple M4, 10 cores, 16 GB). Date: 2026-11-18. Script: `benchmarks/skew.py --months 2013-01 … 2013-06 --replicate 20 --repeat 3`. Spark 3.5.9 `local[*]`, 200 shuffle partitions. The 2024-09 bronze stream was running at the same time.

Input: 2013 H1 silver (`game_id`, `opening_family`, `white_elo`, `white_score`), with every row repeated 20 times → 19,320,560 rows across 287 families. Each copy gets its own `rep`, so a salt hashed from (`game_id`, `rep`) still spreads it. Every key keeps its share of rows, so the skew is real; only the absolute size is scaled up so tasks are long enough to time. The hot key is Sicilian Defense, at 10.33% of rows.

The join is games → a per-family lookup table (287 rows, built as a local table, like a seed or file). Broadcast is off unless stated. AQE is on, with partition coalescing off so task counts change only with the fix being measured.

Task numbers come from the Spark UI REST API (`/stages/{id}/{attempt}/taskList`, executor run time and shuffle read per task), for the stage that reads the skewed shuffle. Wall time is the best of 3 for the whole query, written to the `noop` sink.

| Variant | Wall | Tasks | Shuffle per task: max / median | Task time: max / median | Σ task time |
|---|---|---|---|---|---|
| group-by: count, avg per family | 0.42 s | 200 | 30 / 6 records | 5 / 0.5 ms | 0.18 s |
| window: `row_number()` over family | 2.37 s | 200 | 1,995,280 / 3,130 records (16.0 MB / 26 KB) | 1,543 / 6 ms | 12.8 s |
| join, sort-merge, no fix | 2.43 s | 200 | 1,995,281 / 3,132 records (16.0 MB / 26 KB) | 1,198 / 5 ms | 10.1 s |
| join + AQE skew join (threshold 8 MB, advisory 2 MB) | 2.45 s | 220 | 926,683 / 6,921 records (7.5 MB / 57 KB) | **524** / 6 ms | 11.9 s |
| join + AQE skew join, lookup is a cached group-by output | 2.17 s | 200 | 1,995,280 / 3,130 records | 1,128 / 4 ms | 9.4 s |
| join, salted into 16 buckets | 5.61 s | 200 | 311,535 / 85,097 records (5.9 / 1.6 MB) | **266** / 62 ms | 14.7 s |
| join, lookup broadcast | **1.38 s** | no shuffle of the big side | — | — | — |

What happened:
- **Group-by: no skew, as predicted.** Partial aggregation runs before the shuffle, so each of the 8 map tasks sends one row per family: 1,467 records in total for 19.3M input rows. This is the shape of every gold model (`fct_opening_performance` etc.), so gold has no skew problem in either engine.
- **Window and join: one task gets the whole hot key.** Its shuffle read is 640× the median (2.0M rows vs. 3.1K), and it runs 240–260× longer than the median task. No partial step exists for a window, and AQE's skew handling covers only joins. The fix for a skewed window is a finer partition key, if the logic allows it.
- **AQE skew join didn't fire at first, for two separate reasons, both measured:**
  - Default thresholds. A partition counts as skewed only above 256 MB (`skewedPartitionThresholdInBytes`), and it's cut into ~64 MB pieces (`advisoryPartitionSizeInBytes`). The hot partition here is 16 MB, so at the defaults nothing changed. Below ~256 MB per partition, AQE ignores skew by design.
  - Pre-partitioned input. When the lookup was the cached output of a group-by on `opening_family`, it was already hash-partitioned on the join key. Spark then shuffles only the big side, and the skew-join rule, which needs a shuffle stage on both sides, never applies: same 200 tasks, same 1.1 s straggler.
  - With a plain lookup table and lowered thresholds, AQE split the hot partition into 21 pieces (200 → 220 tasks). The longest task dropped from 1,198 to 524 ms. It didn't get close to the median: the largest piece still held 926,683 rows. I didn't check which key or split left that piece.
- **Salting flattens the reduce side best (longest task 266 ms) but costs the most overall.** The big side's map stage went from 1.1 s to 3.9 s of wall time, with every reduce partition now non-empty. The lookup also has to be copied 16 times. Total task time went up 46%, and wall time was 2.3× the baseline.
- **On this machine, none of the skew fixes changes wall time.** A stage can't finish before its longest task or before Σ task time ÷ cores. With 10 cores, the hot key (10.3% of rows) is about one core's fair share: 10.1 s ÷ 10 = 1.0 s vs. the 1.2 s straggler. So the straggler adds almost nothing.
- **Projection, not measured:** on 100 cores the same stage's floor would be the straggler. That's ~1.2 s with no fix vs. ~0.12 s of fair share, so AQE (0.52 s) or salting (0.27 s) would be a 2–4× win there.
- **The right fix for this join is not a skew fix at all.** The lookup is 287 rows, so broadcast it. Then the big side never shuffles, and this is the fastest variant (1.38 s, −43%). Spark would do that by default, since `autoBroadcastJoinThreshold` is 10 MB.
- Before/after for the resume line: longest join task 1,198 ms → 524 ms (AQE skew join) → 266 ms (salting). Wall time 2.43 s → 1.38 s (broadcast).

| 2024-09 silver, ~88M rows (no replication) | pending: rerun once that month's silver lands | | | | |
