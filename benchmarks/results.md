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
| 2024-09: ~88M rows, ~4 GB | — | pending: silver for this month lands after the stream finishes | | | |

Takeaway so far: at 2013 sizes DuckDB wins 8–11× on a warm query and about 40× on what a dbt run actually pays (start + first query: 0.08 s vs. 2.9–4.5 s). Spark's cost here is fixed per-query overhead (planning, task scheduling, the shuffle stage), not throughput. Going from 1 to 6 months, 8× the rows, Spark's warm time only grows 2.1–2.5× while DuckDB's grows 2.3×, so neither is anywhere near its limit. Tuning shuffle partitions to the core count trims Spark by 18–30% and doesn't change the verdict. The 2024-09 row is the real scale test: 88M rows, roughly 90× 2013 H1, still on one machine.
