# Assignment 2: Mastering Big Data Handling

**Course:** SECP3133 High Performance Data Processing  
**Assignment:** Assignment 2 — Mastering Big Data Handling  

---

## Group Information

| Field | Details |
|---|---|
| Group Name | *Byte* |
| Member 1 | *NUR FIRZANA BINTI BADRUS HISHAM - A23CS0156* |
| Member 2 | *NURAISYAH BINTI MOHD ZIKRE - A23CS0160* |
| Platform | Google Colab + GitHub |

---

## 1. Dataset Description

### 1.1 Dataset Overview

| Field | Details |
|---|---|
| **Dataset Name** | Airline Delay and Cancellation Data, 2009–2018 |
| **Source** | [Kaggle — yuanyuwendymu](https://www.kaggle.com/datasets/yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018) |
| **Domain** | Aviation / Transportation |
| **File Used** | `2018.csv` (single year file) |
| **File Size** | 851.6 MB (actual file on Drive) |
| **Total Records** | 7,213,446 rows |
| **Number of Columns** | 28 columns |
| **Format** | CSV |

### 1.2 Dataset Description

This dataset is published by the United States Bureau of Transportation Statistics and contains detailed on-time performance records for domestic airline flights. Each row represents a single flight and includes information such as departure and arrival times, scheduled vs. actual durations, delay categories (carrier delay, weather delay, NAS delay, security delay, and late aircraft delay), cancellation codes, and carrier identifiers.

The dataset is well-suited for this assignment because:
- It is large enough (851.6 MB per year, ~7.2 million rows) to genuinely stress traditional tools.
- It contains a diverse mix of column types — integer, float, string categories, and boolean — making it ideal for data type optimisation.
- Its structure is consistent and well-documented, allowing meaningful aggregation and comparison analysis.

### 1.3 Key Columns Used

| Column | Description | Type |
|---|---|---|
| `FL_DATE` | Flight date | string |
| `OP_CARRIER` | Airline carrier code | string / category |
| `ORIGIN` | Origin airport code | string / category |
| `DEST` | Destination airport code | string / category |
| `DEP_DELAY` | Departure delay in minutes | float |
| `ARR_DELAY` | Arrival delay in minutes | float |
| `CANCELLED` | Whether the flight was cancelled (1/0) | bool |
| `AIR_TIME` | Airborne time in minutes | float |
| `DISTANCE` | Distance between airports in miles | float |

---

## 2. Library Choices

### 2.1 Library Summary

| # | Library | Role | Version Used |
|---|---|---|---|
| 1 | **Pandas** | Baseline (compulsory) | 2.2.2 |
| 2 | **Dask** | Scalable library — parallel processing | 2026.6.0 |
| 3 | **Polars** | Scalable library — high-speed Rust engine | 1.35.2 |

### 2.2 Justification

**Pandas** is the most widely used Python data manipulation library and serves as the baseline for performance comparison. It processes data eagerly and is single-threaded, which makes it the ideal reference point to measure how much scalable libraries improve upon.

**Dask** was selected as Library 2 because it closely mirrors the Pandas API, making the performance contrast directly interpretable. Dask partitions large datasets into smaller chunks and processes them in parallel across CPU cores, without requiring the entire file to reside in memory. This makes it particularly relevant when working in memory-constrained environments such as Google Colab's free tier.

**Polars** was selected as Library 3 because it represents a fundamentally different architectural approach. Written in Rust and built around the Apache Arrow memory format, Polars supports lazy evaluation and automatic query optimisation. It is consistently among the fastest DataFrame libraries available in Python and provides a striking performance contrast with both Pandas and Dask.

---

## 3. Data Loading and Inspection

### 3.1 Initial Load

We begin by loading a 50,000-row preview of the dataset to inspect its structure without consuming excessive memory.

```python
import pandas as pd

DATA_PATH = '/content/drive/MyDrive/2018.csv'

df_inspect = pd.read_csv(DATA_PATH, nrows=50_000)
print(f'Shape: {df_inspect.shape[0]:,} rows × {df_inspect.shape[1]} columns')
```

**Output:**
```
File size : 851.6 MB
Shape: 50,000 rows × 28 columns
```

### 3.2 Column Names and Data Types

```python
print(df_inspect.dtypes.to_string())
```

**Output (selected columns):**
```
FL_DATE         object
OP_CARRIER      object
ORIGIN          object
DEST            object
DEP_DELAY      float64
ARR_DELAY      float64
CANCELLED      float64
AIR_TIME       float64
DISTANCE       float64
```

The full 28-column output confirms the dataset also carries several delay-cause and timing columns not used in our analysis (`CRS_DEP_TIME`, `TAXI_OUT`, `WHEELS_OFF`, `CARRIER_DELAY`, `WEATHER_DELAY`, `NAS_DELAY`, `SECURITY_DELAY`, `LATE_AIRCRAFT_DELAY`, and a trailing empty `Unnamed: 27` column caused by a stray delimiter at the end of each CSV row). All string columns default to `object` and all numeric columns default to `float64` or `int64` — both are commonly wasteful defaults that we will address in Strategy 3.

### 3.3 Missing Values

```python
missing = df_inspect.isnull().sum()
missing_pct = (missing / len(df_inspect) * 100).round(2)
```

**Output (columns with missing values, out of 50,000 rows):**
```
                     Missing Count  Missing %
DEP_TIME                       977       1.95
DEP_DELAY                     1049       2.10
TAXI_OUT                       999       2.00
WHEELS_OFF                     999       2.00
WHEELS_ON                     1025       2.05
TAXI_IN                       1025       2.05
ARR_TIME                      1025       2.05
ARR_DELAY                     1118       2.24
CANCELLATION_CODE            48993      97.99
ACTUAL_ELAPSED_TIME           1094       2.19
AIR_TIME                      1094       2.19
CARRIER_DELAY                36724      73.45
WEATHER_DELAY                 36724      73.45
NAS_DELAY                     36724      73.45
SECURITY_DELAY                36724      73.45
LATE_AIRCRAFT_DELAY           36724      73.45
Unnamed: 27                   50000     100.00
```

**Key findings:**
- `DEP_DELAY` (2.10%), `ARR_DELAY` (2.24%), and `AIR_TIME` (2.19%) contain a small share of missing values, corresponding to cancelled or diverted flights.
- The delay-cause columns (`CARRIER_DELAY`, `WEATHER_DELAY`, `NAS_DELAY`, `SECURITY_DELAY`, `LATE_AIRCRAFT_DELAY`) are missing for ~73% of rows, since they are only populated when a flight is actually delayed.
- `CANCELLED` and `DISTANCE` are fully populated (0% missing).
- `Unnamed: 27` is 100% empty — an artifact column from a trailing comma in the raw CSV — and is dropped automatically since it is not in `SELECTED_COLS`.
- Missing values will be handled at the strategy level using `.dropna()` where appropriate.

### 3.4 Data Preview

```python
df_inspect.head()
```

The first rows confirm that the dataset contains flight records with departure/arrival timestamps, airline codes, and delay figures. The `CANCELLED` column contains float values (0.0 / 1.0) by default — this will be converted to `bool` during type optimisation.

---

## 4. Big Data Handling Strategies

---

### 4.1 Strategy 1: Load Less Data

#### What and Why

When a CSV file contains 28 columns but our analysis only requires 9, loading all 28 wastes memory immediately at read time. The `usecols` parameter in `pandas.read_csv()` instructs Pandas to parse and store only the specified columns, reducing the memory footprint before any processing begins.

#### Implementation

```python
SELECTED_COLS = [
    'FL_DATE', 'OP_CARRIER', 'ORIGIN', 'DEST',
    'DEP_DELAY', 'ARR_DELAY', 'CANCELLED',
    'AIR_TIME', 'DISTANCE'
]

import tracemalloc, time

# Full load
tracemalloc.start()
t0 = time.time()
df_full = pd.read_csv(DATA_PATH)
t_full = time.time() - t0
_, mem_full = tracemalloc.get_traced_memory()
tracemalloc.stop()

# Selective load
tracemalloc.start()
t0 = time.time()
df_selective = pd.read_csv(DATA_PATH, usecols=SELECTED_COLS)
t_selective = time.time() - t0
_, mem_selective = tracemalloc.get_traced_memory()
tracemalloc.stop()
```

#### Results

**Output:**
```
=== Strategy 1: Load Less Data ===
Full load    → Time: 56.59s | Peak Memory: 5289.8 MB
Select cols  → Time: 19.46s | Peak Memory: 997.1 MB
Memory saved : 4292.7 MB (81.1% reduction)
Rows loaded  : 7,213,446
Columns kept : 9 / originally 28+
```

| Metric | Full Load (28 cols) | Selective Load (9 cols) | Reduction |
|---|---|---|---|
| Peak Memory | 5,289.8 MB | 997.1 MB | 81.1% |
| Load Time | 56.59 s | 19.46 s | 65.6% |
| Columns Loaded | 28 | 9 | — |
| Rows Loaded | 7,213,446 | 7,213,446 | — |

#### Discussion

By simply specifying `usecols`, we immediately reduce peak memory by roughly 81% (from 5,289.8 MB to 997.1 MB) and cut load time by close to two-thirds (56.59 s → 19.46 s). This strategy has zero computational cost and should be the first optimisation applied to any large file read. It is especially impactful when the dataset contains many columns that are irrelevant to the analysis at hand — here, dropping 19 of the 28 columns (including the delay-cause and taxi/wheels-timing columns) more than halved both memory and time.

---

### 4.2 Strategy 2: Chunking

#### What and Why

Chunking involves reading the dataset in fixed-size portions called chunks, processing each chunk sequentially, and then either aggregating results or discarding each chunk before loading the next. At any point, only one chunk occupies memory — making it possible to process files that are larger than available RAM.

#### Implementation

```python
CHUNK_SIZE = 200_000

tracemalloc.start()
t0 = time.time()

total_delay = 0
total_rows  = 0
chunks_read = 0

for chunk in pd.read_csv(DATA_PATH, usecols=SELECTED_COLS, chunksize=CHUNK_SIZE):
    chunk = chunk.dropna(subset=['DEP_DELAY'])
    total_delay += chunk['DEP_DELAY'].sum()
    total_rows  += len(chunk)
    chunks_read += 1

t_chunk = time.time() - t0
_, mem_chunk = tracemalloc.get_traced_memory()
tracemalloc.stop()

avg_delay = total_delay / total_rows
```

#### Results

**Output:**
```
=== Strategy 2: Chunking ===
Chunk size   : 200,000 rows
Chunks read  : 37
Total rows   : 7,096,212
Execution time  : 30.46 s
Peak memory  : 43.9 MB  (only 1 chunk in RAM at a time)
Avg DEP_DELAY: 9.97 minutes
```

| Metric | Value |
|---|---|
| Chunk Size | 200,000 rows |
| Total Chunks Processed | 37 |
| Total Rows Processed | 7,096,212 (after dropping rows with missing `DEP_DELAY`) |
| Peak Memory | 43.9 MB (1 chunk at a time) |
| Execution Time | 30.46 s |
| Computed Avg Departure Delay | 9.97 minutes |

#### Discussion

The key advantage of chunking is that peak memory is bounded by chunk size rather than file size. With a chunk size of 200,000 rows, memory consumption stayed at just 43.9 MB — dramatically lower than the 997.1 MB needed to hold the selected-column dataset in one piece (Strategy 1) — regardless of how large the overall file is. Chunking was also the fastest full-file pass of all the single-machine strategies (30.46 s), since each chunk is small enough to process with minimal overhead. The trade-off is that operations requiring global context (such as a sort across the full dataset) cannot be done directly within the chunk loop — but aggregations, filters, and transformations (like the running sum used here to compute the average delay) are fully supported.

---

### 4.3 Strategy 3: Data Type Optimisation

#### What and Why

When Pandas reads a CSV, it assigns conservative default types: all floats become `float64`, all integers become `int64`, and all strings become `object`. These defaults are memory-heavy. By downcasting numeric columns to smaller types (e.g., `float32`) and converting low-cardinality string columns to `category`, we can reduce the dataset's memory footprint by 50% or more.

#### Implementation

```python
df_default   = pd.read_csv(DATA_PATH, usecols=SELECTED_COLS)
mem_before   = df_default.memory_usage(deep=True).sum() / 1024**2

df_optimised = df_default.copy()

# Low-cardinality strings → category
for col in ['OP_CARRIER', 'ORIGIN', 'DEST']:
    df_optimised[col] = df_optimised[col].astype('category')

# Float64 → float32
for col in ['DEP_DELAY', 'ARR_DELAY', 'AIR_TIME', 'DISTANCE']:
    df_optimised[col] = pd.to_numeric(df_optimised[col], errors='coerce').astype('float32')

# Boolean column
df_optimised['CANCELLED'] = df_optimised['CANCELLED'].astype('bool')

mem_after  = df_optimised.memory_usage(deep=True).sum() / 1024**2
reduction  = (1 - mem_after / mem_before) * 100
```

#### Results

**Output:**
```
Memory BEFORE optimisation: 1747.3 MB
Memory AFTER  optimisation: 557.3 MB
Reduction     : 68.1%
```

| Metric | Before | After | Saved | Reduction |
|---|---|---|---|---|
| **Total Memory** | **1,747.3 MB** | **557.3 MB** | **1,190.0 MB** | **68.1%** |
| `FL_DATE` | object (405.9 MB) | object (405.9 MB) | 0.0 MB | 0% (unchanged — not converted) |
| `OP_CARRIER` | object (350.8 MB) | category (6.9 MB) | 344.0 MB | 98.0% |
| `ORIGIN` | object (357.7 MB) | category (13.8 MB) | 343.9 MB | 96.1% |
| `DEST` | object (357.7 MB) | category (13.8 MB) | 343.9 MB | 96.1% |
| `DEP_DELAY` | float64 (55.0 MB) | float32 (27.5 MB) | 27.5 MB | 50% |
| `ARR_DELAY` | float64 (55.0 MB) | float32 (27.5 MB) | 27.5 MB | 50% |
| `CANCELLED` | float64 (55.0 MB) | bool (6.9 MB) | 48.2 MB | 87.5% |
| `AIR_TIME` | float64 (55.0 MB) | float32 (27.5 MB) | 27.5 MB | 50% |
| `DISTANCE` | float64 (55.0 MB) | float32 (27.5 MB) | 27.5 MB | 50% |

#### Discussion

The most dramatic gains come from converting `OP_CARRIER`, `ORIGIN`, and `DEST` from `object` to `category` — together these three columns account for over 1,030 MB of the total 1,190 MB saved. These columns contain a small number of unique values (airline codes and airport codes) repeated across 7.2 million rows, so storing them as categories (essentially integer codes with a lookup table) is far more efficient than repeating full strings. `CANCELLED` also benefited strongly (87.5% reduction) by converting from `float64` to `bool`, since it only ever holds 0/1 values. Note that `FL_DATE` was intentionally left as `object` in this pass — converting it to `datetime64` or `category` would free up more memory still, since at 405.9 MB it is now the single largest column in the optimised dataframe. This strategy is particularly powerful when combined with `usecols`: together they bring the 851.6 MB CSV down to 557.3 MB in memory — and combined with Strategy 1's selective load (997.1 MB → 557.3 MB), roughly halves the memory footprint again on top of the initial column-pruning gain.

---

### 4.4 Strategy 4: Sampling

#### What and Why

Sampling involves selecting a statistically representative subset of the full dataset for rapid exploration. Rather than waiting minutes for the full dataset to be processed, a 5% sample (approximately 360,000 rows) can be analysed in seconds — and the distributions it produces closely mirror those of the full data.

#### Implementation

```python
SAMPLE_FRACTION = 0.05
RANDOM_SEED     = 42

t0 = time.time()
df_sample = df_optimised.sample(frac=SAMPLE_FRACTION, random_state=RANDOM_SEED).reset_index(drop=True)
t_sample  = time.time() - t0

print(f'Full rows : {len(df_optimised):,}')
print(f'Sample rows: {len(df_sample):,}')
print(f'Sampling time: {t_sample:.4f} s')
```

#### Results

**Output:**
```
=== Strategy 4: Sampling ===
Full dataset rows : 7,213,446
Sample rows       : 360,672  (5%)
Sampling time     : 0.7847 s
Sample memory     : 68.1 MB

Sample — Avg Departure Delay by Carrier (top 10):
OP_CARRIER
F9    20.15
B6    16.59
OH    12.47
G4    12.41
EV    11.85
WN    11.28
9E    10.91
YV    10.85
NK    10.49
AA    10.22
```

| Metric | Full Dataset | 5% Sample |
|---|---|---|
| Rows | 7,213,446 | 360,672 |
| Sampling Time | — | 0.78 s |
| Sample Memory | — | 68.1 MB |

The per-carrier average departure delays computed on the 5% sample (e.g., F9 at ~20.2 min, B6 at ~16.6 min) are close to the values later computed on the full dataset in Strategy 5 (F9 at 19.68 min, B6 at 15.87 min) — a difference of well under a minute for the top carriers, confirming the sample is representative for exploratory analysis. The side-by-side histograms in the notebook (Task 3, Strategy 4) show the departure-delay distributions of the full dataset and the sample overlaid, and they track each other closely.

#### Discussion

Sampling is most valuable during the development and testing phase of a data project. Sampling itself took under a second (0.78 s) — effectively instantaneous compared to the 19–70 second full-dataset reads in the other strategies — which makes it ideal for quickly validating groupby logic like the carrier-level aggregation used throughout this assignment. Once the analysis code is validated on the sample, it can be applied to the full dataset using chunking or a scalable library. The risk is that very rare events (e.g., extreme delays, specific rare cancellation codes) may be under-represented in small samples — stratified sampling can mitigate this for categorical columns.

---

### 4.5 Strategy 5: Parallel Processing with Scalable Libraries

#### What and Why

Standard Pandas operates on a single CPU core. Modern machines have 4–16+ cores, most of which sit idle during a Pandas operation. Dask and Polars both exploit multi-core parallelism — Dask by distributing partitions across cores using a task scheduler, and Polars by using a Rust-native parallel execution engine with SIMD vectorisation.

The benchmark operation performed by all three libraries is identical:
> Read the full dataset (selected columns) → group by airline carrier → compute mean departure delay → sort descending.

---

#### 5a. Pandas (Baseline)

```python
tracemalloc.start()
t0 = time.time()

df_pandas = pd.read_csv(
    DATA_PATH, usecols=SELECTED_COLS,
    dtype={'DEP_DELAY': 'float32', 'ARR_DELAY': 'float32',
           'AIR_TIME': 'float32',  'DISTANCE': 'float32'}
)
result_pandas = (
    df_pandas.groupby('OP_CARRIER')['DEP_DELAY']
    .mean()
    .sort_values(ascending=False)
)

t_pandas = time.time() - t0
_, mem_pandas = tracemalloc.get_traced_memory()
tracemalloc.stop()
```

**Output:**
```
=== Pandas: Mean Departure Delay per Carrier ===
OP_CARRIER
F9    19.68
B6    15.87
G4    12.92
EV    12.32
OH    12.04
...
AS     2.48
HA     0.91

Pandas execution time : 69.31 s
Pandas peak memory    : 1492.5 MB
```

---

#### 5b. Dask

```python
tracemalloc.start()
t0 = time.time()

ddf = dd.read_csv(
    DATA_PATH, usecols=SELECTED_COLS,
    dtype={'DEP_DELAY': 'float32', 'ARR_DELAY': 'float32',
           'AIR_TIME': 'float32',  'DISTANCE': 'float32', 'CANCELLED': 'bool'}
)
result_dask = (
    ddf.groupby('OP_CARRIER')['DEP_DELAY']
    .mean()
    .compute()
    .sort_values(ascending=False)
)

t_dask = time.time() - t0
_, mem_dask = tracemalloc.get_traced_memory()
tracemalloc.stop()
```

**Output:**
```
=== Dask: Mean Departure Delay per Carrier ===
OP_CARRIER
F9    19.68
B6    15.87
G4    12.92
EV    12.32
OH    12.04
...
AS     2.48
HA     0.91

Dask execution time : 52.02 s
Dask peak memory    : 458.5 MB
```

> **Note:** Dask operations are lazy by default. The `.compute()` call is what triggers actual execution and must always be included to obtain results. The carrier-level results are identical to Pandas, confirming correctness — Dask's advantage here is purely in speed and memory, not in the answer produced.

---

#### 5c. Polars

```python
tracemalloc.start()
t0 = time.time()

result_polars = (
    pl.scan_csv(DATA_PATH)
    .select(SELECTED_COLS)
    .group_by('OP_CARRIER')
    .agg(pl.col('DEP_DELAY').mean())
    .sort('DEP_DELAY', descending=True)
    .collect()
)

t_polars = time.time() - t0
_, mem_polars = tracemalloc.get_traced_memory()
tracemalloc.stop()
```

**Output:**
```
=== Polars: Mean Departure Delay per Carrier ===
shape: (18, 2)
┌────────────┬───────────┐
│ OP_CARRIER ┆ DEP_DELAY │
│ ---        ┆ ---       │
│ str        ┆ f64       │
╞════════════╪═══════════╡
│ F9         ┆ 19.684102 │
│ B6         ┆ 15.869267 │
│ G4         ┆ 12.922575 │
│ EV         ┆ 12.319338 │
│ OH         ┆ 12.038332 │
│ …          ┆ …         │
│ DL         ┆ 7.461923  │
│ YX         ┆ 7.339655  │
│ VX         ┆ 6.333836  │
│ AS         ┆ 2.48182   │
│ HA         ┆ 0.913099  │
└────────────┴───────────┘

Polars execution time : 7.63 s
Polars peak memory    : 0.0 MB
```

> **Note:** `pl.scan_csv()` creates a lazy query plan. No data is read until `.collect()` is called. Polars automatically applies predicate pushdown and column pruning at the scan level. The measured peak memory of 0.0 MB is a measurement artifact rather than a literal reading: `tracemalloc` only tracks memory allocated through Python's own allocator, and Polars' Rust engine allocates its buffers outside of that allocator, so its true memory use is invisible to this particular tool. This is discussed further in Section 5.3.

---

## 5. Comparative Analysis

### 5.1 Performance Metrics

#### Execution Time

| Library | Time (seconds) | Speedup vs Pandas |
|---|---|---|
| Pandas (Baseline) | 69.31 s | 1.0× (reference) |
| Dask | 52.02 s | 1.33× faster |
| Polars | 7.63 s | 9.08× faster |

#### Peak Memory Usage

| Library | Peak Memory (MB) | Reduction vs Pandas |
|---|---|---|
| Pandas (Baseline) | 1,492.5 MB | — (reference) |
| Dask | 458.5 MB | 3.25× less (69.3% less) |
| Polars | 0.0 MB* | not meaningfully comparable* |

*As noted in Section 4.5c, `tracemalloc` cannot see memory allocated by Polars' Rust engine, so its 0.0 MB reading understates true usage rather than proving Polars used no memory. Qualitatively, Polars' lazy scan with column pruning and predicate pushdown is expected to use less memory than Pandas, but the exact figure would require a native memory profiler (e.g., `memory_profiler` sampling RSS, or the OS-level peak RSS) rather than `tracemalloc`.

#### Processing Efficiency

| Criterion | Pandas | Dask | Polars |
|---|---|---|---|
| Parallel execution | No | Yes | Yes |
| Lazy evaluation | No | Yes | Yes |
| API familiarity | Native | Pandas-like | New syntax |
| Error handling | Straightforward | Occasional `.compute()` errors | Strict type enforcement |
| Scalability to 10 GB | Would crash | Handles well | Handles well |
| Scalability to 100 GB | Not viable | Requires config | Requires streaming |

### 5.2 Visual Comparison

*(Charts are generated in the notebook — see `big_data.ipynb`, Task 4: Comparative Analysis)*

The bar charts in the notebook clearly show:
1. Polars completes the read + groupby task in about one-ninth the time of Pandas (7.63 s vs 69.31 s).
2. Dask uses roughly a third of Pandas' recorded peak memory (458.5 MB vs 1,492.5 MB) for the same operation.
3. Polars' recorded memory reads as 0.0 MB on the chart — a visible artifact of `tracemalloc` not tracking its Rust-side allocations, rather than genuine zero usage (see the caveat in Section 5.1).

### 5.3 Critical Discussion

**Output (speedup ratios, from the notebook):**
```
=== Speedup Ratios vs Pandas ===
Dask   is 1.33x faster  |  3.25x more memory-efficient
Polars is 9.08x faster  |  35366.99x more memory-efficient
```

The Dask figures (1.33× faster, 3.25× more memory-efficient) are straightforward and reliable. The Polars memory-efficiency figure of 35,366.99× is not — it is the direct result of dividing Pandas' 1,492.5 MB by Polars' 0.0 MB `tracemalloc` reading, which (as explained above) reflects a blind spot in the measurement tool rather than a genuine 35,000-fold memory saving. The 9.08× speed figure, by contrast, is a wall-clock timing and is trustworthy.

**Why is Polars so much faster than Pandas?**

Polars is written in Rust — a compiled, low-level systems language — while Pandas is written in Python with C extensions. Polars also uses the Apache Arrow columnar memory format, which enables SIMD (Single Instruction Multiple Data) vectorised operations: the processor applies one instruction to many data elements simultaneously. Additionally, `pl.scan_csv()` applies *predicate pushdown* and *column pruning* at the file scan level, meaning it never reads columns or rows it does not need. The result is a pipeline where the CPU, memory bus, and I/O subsystem are all utilised efficiently.

**Why is Dask faster than Pandas but slower than Polars?**

Dask achieves parallelism by splitting the CSV into partitions and processing them concurrently across CPU cores. However, Dask still uses Pandas DataFrames internally for each partition, so it inherits Pandas' per-partition overhead. The task scheduler also introduces coordination overhead when merging partition results. Dask's advantage is primarily from parallelism rather than algorithmic efficiency.

**What are the trade-offs?**

- Polars has a different API from Pandas, requiring developers to learn new syntax (e.g., `pl.col()`, `.lazy()`, `.collect()`). This has a learning curve.
- Dask's lazy model can cause confusing errors when `.compute()` is omitted or when operations are not supported on lazy graphs.
- Pandas remains the most beginner-friendly and has the largest ecosystem of compatible tools and tutorials.

---

## 6. Conclusion and Reflection

### 6.1 Summary of Key Observations

The assignment highlights the significant performance advantages of modern scalable libraries and proactive memory management when handling large datasets, noting that "Load Less Data" and "Data Type Optimisation" provide substantial memory reductions of 81.1% and 68.1% respectively. While chunking and sampling offer essential methods for managing memory-constrained environments and enabling rapid exploratory analysis, parallel processing tools provide the most significant performance gains. Ultimately, Polars emerged as the most efficient option, outperforming the Pandas baseline by 9.08× in execution time, while Dask provided a valuable balance of speed and memory efficiency through multi-core parallelism.

### 6.2 Reflection on Learning

1. NUR FIRZANA BINTI BADRUS HISHAM
   * This assignment fundamentally shifted my perspective on performance optimisation; I previously assumed library selection was the primary factor, but I learned that correctly applying usecols and downcasting data types in Pandas provides massive, compound memory savings—often reducing footprints by over 80% before even needing to switch to more scalable tools like Dask or Polars. 

2. NURAISYAH BINTI MOHD ZIKRE
   * I found the transition from eager to lazy evaluation to be the most significant takeaway, particularly seeing how Polars' scan_csv and Dask’s task graphs defer computation until explicitly called, which is a powerful mental model for handling datasets that far exceed system RAM compared to traditional, memory-intensive Pandas workflows.

### 6.3 Scalability Discussion

The strategies applied in this assignment scale differently as dataset size grows:

| Dataset Size | Pandas | Chunking | Dask | Polars | Recommended |
|---|---|---|---|---|---|
| ~850 MB (this assignment) | Viable (69.31 s, 1,492.5 MB) | Viable (30.46 s, 43.9 MB) | Faster (52.02 s, 458.5 MB) | Fastest (7.63 s) | Polars |
| 10 GB | Crash likely | Viable | Good | Good | Dask / Polars |
| 100 GB | Not viable | Slow | Needs cluster | Needs streaming | Spark / Ray |
| 1 TB+ | Not viable | Not viable | Needs large cluster | Not viable | BigQuery / Databricks |

At 10 GB, Dask and Polars with streaming would remain appropriate. Beyond 100 GB, distributed frameworks such as **Apache Spark** (which distributes processing across multiple machines) or cloud-native query engines such as **Google BigQuery** or **Snowflake** become necessary. The strategies explored here — chunking, type optimisation, lazy evaluation, and parallel processing — remain conceptually relevant even at those scales, as they underpin how distributed frameworks are designed.

---

## References

Yuanyuwendymu (2019). *Airline Delay and Cancellation Data, 2009–2018*. Kaggle. https://www.kaggle.com/datasets/yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018
