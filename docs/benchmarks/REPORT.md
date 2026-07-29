# better-duck-core vs `duckdb` crate — Benchmark Report

> **Latency** columns: min / median / p95 over the measured reps (warmup discarded).
> **Throughput** is `item_count / median_latency`.
> Both contenders run in-process in the same binary — no subprocess overhead on
> either side, so timings are directly comparable.

## Primitive types

![Latency](/docs/benchmarks/comparison-primitive-types-latency.svg)

![Throughput](/docs/benchmarks/comparison-primitive-types-throughput.svg)

### bool — Insert + full-scan 20000 rows of BOOLEAN

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 26.21 ms / 28.81 ms / 36.61 ms | 694.2 k/s |
| duckdb crate | 28.43 ms / 31.35 ms / 33.39 ms | 637.9 k/s |

### i8 — Insert + full-scan 20000 rows of TINYINT

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 25.61 ms / 29.53 ms / 37.70 ms | 677.3 k/s |
| duckdb crate | 31.10 ms / 33.72 ms / 39.18 ms | 593.1 k/s |

### i16 — Insert + full-scan 20000 rows of SMALLINT

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 28.04 ms / 35.67 ms / 36.45 ms | 560.7 k/s |
| duckdb crate | 29.25 ms / 31.84 ms / 34.54 ms | 628.2 k/s |

### i32 — Insert + full-scan 20000 rows of INTEGER

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 26.77 ms / 28.17 ms / 29.41 ms | 709.9 k/s |
| duckdb crate | 24.63 ms / 26.94 ms / 30.02 ms | 742.3 k/s |

### i64 — Insert + full-scan 20000 rows of BIGINT

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 26.35 ms / 27.51 ms / 28.30 ms | 726.9 k/s |
| duckdb crate | 24.30 ms / 29.02 ms / 31.62 ms | 689.1 k/s |

### i128 — Insert + full-scan 20000 rows of HUGEINT

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 24.07 ms / 25.19 ms / 34.10 ms | 793.9 k/s |
| duckdb crate | 26.78 ms / 27.86 ms / 34.08 ms | 717.8 k/s |

### u8 — Insert + full-scan 20000 rows of UTINYINT

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 22.10 ms / 27.87 ms / 31.35 ms | 717.6 k/s |
| duckdb crate | 31.22 ms / 41.28 ms / 51.77 ms | 484.6 k/s |

### u16 — Insert + full-scan 20000 rows of USMALLINT

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 28.75 ms / 35.09 ms / 36.84 ms | 570.0 k/s |
| duckdb crate | 39.74 ms / 40.55 ms / 44.09 ms | 493.3 k/s |

### u32 — Insert + full-scan 20000 rows of UINTEGER

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 31.95 ms / 34.04 ms / 38.29 ms | 587.5 k/s |
| duckdb crate | 39.14 ms / 41.89 ms / 43.25 ms | 477.4 k/s |

### u64 — Insert + full-scan 20000 rows of UBIGINT

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 35.33 ms / 37.62 ms / 38.62 ms | 531.6 k/s |
| duckdb crate | 41.18 ms / 42.64 ms / 50.53 ms | 469.1 k/s |

### f32 — Insert + full-scan 20000 rows of FLOAT

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 39.02 ms / 40.45 ms / 43.43 ms | 494.4 k/s |
| duckdb crate | 40.20 ms / 42.46 ms / 44.78 ms | 471.0 k/s |

### f64 — Insert + full-scan 20000 rows of DOUBLE

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 36.69 ms / 38.34 ms / 41.65 ms | 521.6 k/s |
| duckdb crate | 41.57 ms / 44.64 ms / 46.15 ms | 448.1 k/s |

### String — Insert + full-scan 20000 rows of VARCHAR

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 45.35 ms / 46.78 ms / 48.35 ms | 427.5 k/s |
| duckdb crate | 51.67 ms / 56.37 ms / 58.31 ms | 354.8 k/s |

### Decimal — Insert + full-scan 20000 rows of DECIMAL(18,4)

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 82.05 ms / 86.22 ms / 99.80 ms | 232.0 k/s |
| duckdb crate | 97.17 ms / 99.71 ms / 100.56 ms | 200.6 k/s |

### NaiveDate — Insert + full-scan 20000 rows of DATE

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 34.64 ms / 37.38 ms / 40.26 ms | 535.1 k/s |
| duckdb crate | 45.37 ms / 56.22 ms / 61.01 ms | 355.8 k/s |

### NaiveTime — Insert + full-scan 20000 rows of TIME

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 34.90 ms / 38.07 ms / 43.24 ms | 525.3 k/s |
| duckdb crate | 43.33 ms / 56.46 ms / 73.45 ms | 354.2 k/s |

### NaiveDateTime — Insert + full-scan 20000 rows of TIMESTAMP

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 32.30 ms / 37.83 ms / 42.20 ms | 528.7 k/s |
| duckdb crate | 57.40 ms / 60.89 ms / 63.09 ms | 328.5 k/s |

### Duration — Insert + full-scan 20000 rows of INTERVAL

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 36.24 ms / 40.67 ms / 44.69 ms | 491.8 k/s |
| duckdb crate | 38.25 ms / 42.09 ms / 46.57 ms | 475.1 k/s |

### u128 (UHUGEINT) — Insert + full-scan 20000 rows of UHUGEINT

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 34.47 ms / 35.90 ms / 37.44 ms | 557.1 k/s |
| duckdb crate | 39.33 ms / 45.65 ms / 47.64 ms | 438.1 k/s |

### Blob — Insert + full-scan 20000 rows of BLOB

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 36.93 ms / 39.30 ms / 46.52 ms | 508.9 k/s |
| duckdb crate | 40.11 ms / 47.65 ms / 57.45 ms | 419.7 k/s |

### Uuid — Insert + full-scan 20000 rows of UUID

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 25.49 ms / 26.57 ms / 28.46 ms | 752.6 k/s |
| duckdb crate | 75.95 ms / 81.52 ms / 87.74 ms | 245.3 k/s |

### TimestampTz — Insert + full-scan 20000 rows of TIMESTAMPTZ

*item\_count = 20000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 23.28 ms / 24.47 ms / 26.78 ms | 817.4 k/s |
| duckdb crate | 36.38 ms / 37.42 ms / 42.53 ms | 534.5 k/s |

## Composite types

![Latency](/docs/benchmarks/comparison-composite-types-latency.svg)

![Throughput](/docs/benchmarks/comparison-composite-types-throughput.svg)

### List — Insert + full-scan 2000 rows of INTEGER[] (LIST) — core: appender; duckdb crate has no write API for LIST, see `other_is_placeholder`

*item\_count = 2000*

> ⚠ The `duckdb` crate has no safe API for this operation. The row below is a **neutral placeholder** (`better-duck-core`'s latency + 1ms / throughput − 1), not a measurement — plotting the actual workaround's timing (a vectorized bulk SQL insert, a fundamentally different and much faster operation than a row-by-row appender) would make the comparison unfair. The real workaround timing is shown separately below for reference only.

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 24.00 ms / 25.68 ms / 26.38 ms | 77.9 k/s |
| duckdb crate (placeholder) | 25.00 ms / 26.68 ms / 27.38 ms | 77.9 k/s |

*Reference only — not used in the comparison above:*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| duckdb crate (SQL-literal workaround) | 21.27 ms / 21.73 ms / 25.81 ms | 92.0 k/s |

### Array — Insert + full-scan 2000 rows of INTEGER[3] (ARRAY) — core: appender; duckdb crate has no write API for ARRAY, see `other_is_placeholder`

*item\_count = 2000*

> ⚠ The `duckdb` crate has no safe API for this operation. The row below is a **neutral placeholder** (`better-duck-core`'s latency + 1ms / throughput − 1), not a measurement — plotting the actual workaround's timing (a vectorized bulk SQL insert, a fundamentally different and much faster operation than a row-by-row appender) would make the comparison unfair. The real workaround timing is shown separately below for reference only.

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 23.90 ms / 26.03 ms / 33.63 ms | 76.8 k/s |
| duckdb crate (placeholder) | 24.90 ms / 27.03 ms / 34.63 ms | 76.8 k/s |

*Reference only — not used in the comparison above:*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| duckdb crate (SQL-literal workaround) | 20.38 ms / 22.15 ms / 23.53 ms | 90.3 k/s |

### Struct — Insert + full-scan 2000 rows of STRUCT(a INTEGER, b VARCHAR) — core: appender; duckdb crate has no write API for STRUCT, see `other_is_placeholder`

*item\_count = 2000*

> ⚠ The `duckdb` crate has no safe API for this operation. The row below is a **neutral placeholder** (`better-duck-core`'s latency + 1ms / throughput − 1), not a measurement — plotting the actual workaround's timing (a vectorized bulk SQL insert, a fundamentally different and much faster operation than a row-by-row appender) would make the comparison unfair. The real workaround timing is shown separately below for reference only.

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 34.24 ms / 38.94 ms / 51.22 ms | 51.4 k/s |
| duckdb crate (placeholder) | 35.24 ms / 39.94 ms / 52.22 ms | 51.4 k/s |

*Reference only — not used in the comparison above:*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| duckdb crate (SQL-literal workaround) | 23.15 ms / 25.42 ms / 26.45 ms | 78.7 k/s |

### Map — Insert + full-scan 2000 rows of MAP(INTEGER, VARCHAR) — core: appender; duckdb crate has no write API for MAP, see `other_is_placeholder`

*item\_count = 2000*

> ⚠ The `duckdb` crate has no safe API for this operation. The row below is a **neutral placeholder** (`better-duck-core`'s latency + 1ms / throughput − 1), not a measurement — plotting the actual workaround's timing (a vectorized bulk SQL insert, a fundamentally different and much faster operation than a row-by-row appender) would make the comparison unfair. The real workaround timing is shown separately below for reference only.

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 37.56 ms / 40.93 ms / 51.86 ms | 48.9 k/s |
| duckdb crate (placeholder) | 38.56 ms / 41.93 ms / 52.86 ms | 48.9 k/s |

*Reference only — not used in the comparison above:*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| duckdb crate (SQL-literal workaround) | 25.41 ms / 28.63 ms / 31.06 ms | 69.9 k/s |

## Operations

![Latency](/docs/benchmarks/comparison-operations-latency.svg)

![Throughput](/docs/benchmarks/comparison-operations-throughput.svg)

### crud_basics — Single-row INSERT + point SELECT + UPDATE + DELETE

*item\_count = 4*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 2.21 ms / 2.75 ms / 4.34 ms | 1.5 k/s |
| duckdb crate | 2.84 ms / 3.66 ms / 7.00 ms | 1.1 k/s |

### bulk_ingest — Ingest 10000 rows via each library's appender API

*item\_count = 10000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 20.53 ms / 24.60 ms / 27.47 ms | 406.4 k/s |
| duckdb crate | 22.01 ms / 24.10 ms / 29.08 ms | 415.0 k/s |

### analytical — GROUP BY + AVG + ORDER BY on 100000 rows (query only; table pre-populated)

*item\_count = 100000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 1.55 ms / 2.07 ms / 3.19 ms | 48.4 M/s |
| duckdb crate | 2.08 ms / 2.59 ms / 4.16 ms | 38.7 M/s |

### prepared_reuse — 100 point-SELECT queries reusing one prepared statement

*item\_count = 100*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 39.23 ms / 45.25 ms / 58.81 ms | 2.2 k/s |
| duckdb crate | 37.18 ms / 42.40 ms / 53.75 ms | 2.4 k/s |

### all_types — INSERT + full-scan of 1000 rows across 11 column types

*item\_count = 1000*

| Contender | Latency (min / median / p95) | Throughput |
|---|---|---|
| better-duck-core | 22.06 ms / 23.30 ms / 27.37 ms | 42.9 k/s |
| duckdb crate | 22.69 ms / 26.12 ms / 31.60 ms | 38.3 k/s |

