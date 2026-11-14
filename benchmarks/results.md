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
