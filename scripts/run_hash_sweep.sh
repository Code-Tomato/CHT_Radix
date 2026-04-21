#!/usr/bin/env bash
# Main hash-table + trace sweeps (uniform, zipfian, trace, read-mix, cache-resident).
# For radix / shared-prefix workloads use scripts/run_radix_sweep.sh.

set -euo pipefail

BENCH=${BENCH:-./build/bench}
THREADS=(${THREADS:-1 2 4 8 16})
TABLES=(${TABLES:-chaining_coarse chaining_fine cuckoo_optimistic cuckoo_striped hopscotch})
# Sweep-1 already covers the (uniform, 8 threads, read_frac=0.95) cell, so
# we deliberately omit 0.95 here to keep exactly REPEATS rows per unique
# (table, workload, threads, read_frac_config, key_space) group in the CSV.
READ_FRACS=(${READ_FRACS:-1.0 0.5 0.1})
REPEATS=${REPEATS:-3}
OPS=${OPS:-2000000}
WARMUP_OPS=${WARMUP_OPS:-10000}
TRACE_PATH=${TRACE_PATH:-data/sharegpt.trace}
BASE_SEED=${BASE_SEED:-42}
CACHE_RESIDENT_KEYS=${CACHE_RESIDENT_KEYS:-100000}
TS=$(date +%Y%m%d_%H%M%S)
OUT=${OUT:-results/experiments_${TS}.csv}
META=${META:-results/experiments_${TS}.meta.json}
# If PROMOTE_AS_CANONICAL=1 the sweep will rename the outputs to
# results/experiments_canonical.{csv,meta.json} at the end.
PROMOTE_AS_CANONICAL=${PROMOTE_AS_CANONICAL:-0}

mkdir -p results

GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "unversioned")
STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# Header-only stub row so downstream consumers can discover the CSV schema.
# Explicit --warmup-ops=0 keeps the zero-measurement warning quiet for this
# intentionally tiny invocation.
"$BENCH" --csv --csv-header --table=stub --ops=1000 --prepopulate=100 \
  --warmup-ops=0 --seed="$BASE_SEED" --repeat=0 --git-sha="$GIT_SHA" >"$OUT"

run_one() {
  local rep="$1"; shift
  local seed=$((BASE_SEED + rep))
  "$BENCH" --csv --seed="$seed" --repeat="$rep" --git-sha="$GIT_SHA" \
    --warmup-ops="$WARMUP_OPS" "$@" >>"$OUT"
}

for rep in $(seq 1 "$REPEATS"); do
  echo "=== repeat ${rep}/${REPEATS} ===" >&2

  # Sweep 1: uniform @95% reads vs threads
  for t in "${TABLES[@]}"; do
    for n in "${THREADS[@]}"; do
      run_one "$rep" --table="$t" --workload=uniform --threads="$n" \
        --ops="$OPS" --read-fraction=0.95 --prepopulate=100000
    done
  done

  # Sweep 2: zipfian @95% reads vs threads
  for t in "${TABLES[@]}"; do
    for n in "${THREADS[@]}"; do
      run_one "$rep" --table="$t" --workload=zipfian --threads="$n" \
        --ops="$OPS" --read-fraction=0.95 --zipf-alpha=0.99 --prepopulate=100000
    done
  done

  # Sweep 3: trace replay vs threads
  if [[ -f "$TRACE_PATH" ]]; then
    for t in "${TABLES[@]}"; do
      for n in "${THREADS[@]}"; do
        run_one "$rep" --table="$t" --workload=trace --trace="$TRACE_PATH" \
          --threads="$n" --prepopulate=0
      done
    done
  else
    echo "Skipping trace sweep: $TRACE_PATH not found" >&2
  fi

  # Sweep 4: read fraction sensitivity at 8 threads.
  for t in "${TABLES[@]}"; do
    for r in "${READ_FRACS[@]}"; do
      run_one "$rep" --table="$t" --workload=uniform --threads=8 \
        --ops="$OPS" --read-fraction="$r" --prepopulate=100000
    done
  done

  # Sweep 5: cache-resident key_space at 8 threads (small working set).
  for t in "${TABLES[@]}"; do
    run_one "$rep" --table="$t" --workload=uniform --threads=8 \
      --ops="$OPS" --read-fraction=0.95 \
      --key-space="$CACHE_RESIDENT_KEYS" --prepopulate=50000 \
      --initial-capacity=200000
  done
done

if [[ "$(uname)" == "Linux" ]]; then
  for t in "${TABLES[@]}"; do
    for n in 1 8 16; do
      perf stat -e cache-misses,cache-references,L1-dcache-load-misses \
        "$BENCH" --csv --table="$t" --workload=uniform --threads="$n" \
        --ops=10000000 --read-fraction=0.95 --prepopulate=100000 \
        --seed="$BASE_SEED" --repeat=0 --git-sha="$GIT_SHA" \
        2>>"results/perf_${t}_t${n}.log"
    done
  done
else
  echo "Skipping perf counter sweep (non-Linux platform)." >&2
fi

FINISHED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

TRACE_SHA256="absent"
if [[ -f "$TRACE_PATH" ]]; then
  # Content-only SHA: skips the timestamped v2 provenance header so the hash
  # is stable across regenerations of the same input data.
  TRACE_SHA256=$(python3 -c "from pathlib import Path; import sys; sys.path.insert(0, 'src/trace'); from trace_format import content_sha256; print(content_sha256(Path('$TRACE_PATH')))" 2>/dev/null || echo "absent")
fi

if [[ "$PROMOTE_AS_CANONICAL" == "1" ]]; then
  CANONICAL_CSV=results/experiments_canonical.csv
  CANONICAL_META=results/experiments_canonical.meta.json
  mv "$OUT" "$CANONICAL_CSV"
  OUT="$CANONICAL_CSV"
  META="$CANONICAL_META"
fi

python3 - \
  "$META" "$OUT" "$TRACE_PATH" "$TRACE_SHA256" \
  "$GIT_SHA" "$STARTED_AT" "$FINISHED_AT" \
  "$REPEATS" "$BASE_SEED" "$OPS" "$WARMUP_OPS" "$CACHE_RESIDENT_KEYS" \
  "${TABLES[*]}" "${THREADS[*]}" "${READ_FRACS[*]}" <<'PYEOF'
import json
import sys

(
    meta_path, csv_path, trace_path, trace_sha256,
    git_sha, started_at, finished_at,
    repeats, base_seed, ops_per_run, warmup_ops, cache_resident_keys,
    tables_str, threads_str, read_fracs_str,
) = sys.argv[1:]

meta = {
    "csv": csv_path,
    "trace_path": trace_path,
    "trace_sha256": trace_sha256,
    "repeats": int(repeats),
    "base_seed": int(base_seed),
    "ops_per_run": int(ops_per_run),
    "warmup_ops_per_thread": int(warmup_ops),
    "cache_resident_keys": int(cache_resident_keys),
    "git_sha": git_sha,
    "started_at": started_at,
    "finished_at": finished_at,
    "tables": tables_str.split(),
    "threads": threads_str.split(),
    "read_fractions": read_fracs_str.split(),
}
with open(meta_path, "w") as f:
    json.dump(meta, f, indent=2)
    f.write("\n")
PYEOF

echo "Wrote $OUT"
echo "Wrote $META"
