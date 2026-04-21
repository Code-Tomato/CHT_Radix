# Reproducibility Guide

Every number in the write-up [paper/CHT_Radix.pdf](paper/CHT_Radix.pdf) and every plot in `results/` is
produced by the commands below. Start from a clean checkout.

## 0. Dependencies

- C++17 compiler (AppleClang or GCC), CMake ≥ 3.15, POSIX threads
- Python 3.9+
- Optional: Linux `perf` for cache counters

```bash
python3 -m pip install -r requirements.txt
```

## 1. Build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

Optional ThreadSanitizer build for the lock-based tables:

```bash
cmake -B build-tsan -DUSE_TSAN=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build-tsan -j
./build-tsan/correctness_test --threads=8 \
  --tables=stub,chaining_coarse,chaining_fine,cuckoo_striped
```

(TSan flags expected data races on the optimistic seqlock; that variant is
intentionally excluded from TSan runs.)

## 2. Correctness

```bash
./build/correctness_test --threads=16 --repeats=3 \
  --tables=stub,chaining_coarse,chaining_fine,cuckoo_optimistic,cuckoo_striped,hopscotch,radix_tree_coarse,radix_tree_fine
```

For the optimistic cuckoo, `correctness_test` additionally runs a displacement
stress test and an inserter/lookup race test.

## 3. Generate the LLM trace

```bash
python3 src/trace/extract.py --mode=sharegpt --out data/sharegpt.trace \
  --max-ops 1000000 --replay-on-hit 2 --lru-cap 320000
python3 src/trace/verify.py data/sharegpt.trace
```

The trace's v2 binary header records full provenance (`mode`, `source`
dataset id, `tiktoken_encoding`, `block_size`, `lru_cap`, `replay_on_hit`,
`generated_at`). `verify.py` prints it.

### Checking you got the canonical trace

The canonical trace used for the committed plots and paper numbers has a
fixed SHA256, committed at `data/sharegpt.trace.sha256`. After regenerating
the trace you can check:

```bash
python3 src/trace/verify.py data/sharegpt.trace \
  --verify-sha=data/sharegpt.trace.sha256
# or, against the full run meta JSON:
python3 src/trace/verify.py data/sharegpt.trace \
  --verify-sha=results/experiments_canonical.meta.json
```

If the SHA differs, HuggingFace's ShareGPT snapshot has drifted; your
benchmark numbers will be close to but not bit-for-bit identical to the
committed ones. For strict byte-level reproducibility use
`--mode=synthetic` with the fixed seed, which produces the same bytes
every time regardless of upstream data changes:

```bash
python3 src/trace/extract.py --mode=synthetic --out data/sharegpt.trace \
  --max-ops 1000000 --lru-cap 320000 --replay-on-hit 2
```

Both modes record their provenance in the trace header; `verify.py` prints
it so the downstream meta JSON always names which generator produced the
trace it measured.

If ShareGPT download / tokenization fails or exceeds the 20-minute timebox,
the extractor falls back to the synthetic generator and records
`mode=synthetic` in the header explicitly — no silent fallback.

## 4. Run sweeps

Two separate scripts:

- [`scripts/run_hash_sweep.sh`](scripts/run_hash_sweep.sh) — hash tables on uniform, zipfian, trace, read-mix, and cache-resident slices.
- [`scripts/run_radix_sweep.sh`](scripts/run_radix_sweep.sh) — shared-prefix + uniform baseline (hash + radix tables).

### Hash-table + trace sweep

```bash
PROMOTE_AS_CANONICAL=1 ./scripts/run_hash_sweep.sh
```

Outputs:

- `results/experiments_canonical.csv` - one row per (table, workload, threads,
  read_frac, key_space, repeat) combination. See column list below.
- `results/experiments_canonical.meta.json` - run-level metadata including
  `repeats`, `base_seed`, `ops_per_run`, `warmup_ops_per_thread`,
  `cache_resident_keys`, `git_sha`, `started_at`, `finished_at`,
  and a SHA256 of the trace used.

The canonical sweep used for the committed plots and paper numbers was run
with these pins (override with the env vars below if reproducing):

- `REPEATS=3`
- `OPS=500000` (ops per run, across all threads)
- `WARMUP_OPS=5000` (per-thread warmup; the measurement phase advances past it)
- `BASE_SEED=42` (seed per repeat: `BASE_SEED + repeat_idx`)
- `CACHE_RESIDENT_KEYS=100000` (Sweep 5 small working set)

All other env overrides: `THREADS`, `TABLES`, `READ_FRACS`, `TRACE_PATH`.

Without `PROMOTE_AS_CANONICAL=1`, the sweep writes timestamped filenames
(`results/experiments_<TS>.csv` etc.) and leaves the canonical files
untouched.

### Radix shared-prefix sweep

```bash
PROMOTE_AS_CANONICAL=1 ./scripts/run_radix_sweep.sh
```

Writes `results/radix_sweep_canonical.{csv,meta.json}` when promoted; otherwise
`results/radix_sweep_<TS>.csv`. Override with `TABLES`, `PREFIX_COUNTS`,
`THREADS`, `OPS`, `UNIVERSE`, etc. (see `scripts/run_radix_sweep.sh`).

### CSV schema

```
table, workload, threads, ops,
read_frac_config, read_frac_measured, key_space,
throughput_mops, p50_ns, p99_ns, p999_ns, max_ns,
insert_attempts, insert_successes,
remove_attempts, remove_successes,
lookup_attempts, lookup_hits,
wall_seconds, seed, repeat, git_sha
```

`read_frac_config` is the CLI knob. For `workload=trace` the knob is
meaningless (the trace file dictates the op mix) and the column carries
whatever default was passed; trace analysis should use
`read_frac_measured` instead. `read_frac_measured` is the actual
post-warmup operation mix across threads for every workload. `max_ns` is
the true observed maximum, not a bin edge.

## 5. Plot results

```bash
python3 scripts/plot.py hash \
  --csv results/experiments_<TS>.csv --out results
```

Produces:

- `results/throughput_vs_threads.pdf`
- `results/p99_vs_threads.pdf`
- `results/throughput_vs_read_fraction.pdf`
- `results/throughput_cache_resident.pdf` (paired-bar 10M vs 100K key-space)

After a radix shared-prefix sweep (`./scripts/run_radix_sweep.sh`):

```bash
python3 scripts/plot.py radix \
  --csv results/radix_sweep_<TS>.csv --out results
```

Produces `radix_throughput_vs_prefix_count.pdf` and `radix_scaling_1t_8t.pdf`.

## 6. Paper

The write-up in this repository is the PDF [paper/CHT_Radix.pdf](paper/CHT_Radix.pdf). There is no LaTeX build script here.

## Expected numbers (paper-alignment)

Per-table throughput and p99 values cited in the paper are taken from the
committed sweep in [`results/experiments_canonical.csv`](results/experiments_canonical.csv)
(run metadata: [`results/experiments_canonical.meta.json`](results/experiments_canonical.meta.json)).
Radix shared-prefix figures use the same CSV schema in
`results/radix_sweep_*.csv` when you run [`scripts/run_radix_sweep.sh`](scripts/run_radix_sweep.sh).
