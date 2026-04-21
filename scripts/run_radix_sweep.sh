#!/usr/bin/env bash
# Radix / shared-prefix sweeps plus a uniform baseline. For hash+trace sweeps
# use scripts/run_hash_sweep.sh.

set -euo pipefail

BENCH=${BENCH:-./build/bench}
TABLES=(${TABLES:-cuckoo_optimistic chaining_fine radix_tree_coarse radix_tree_fine})
PREFIX_COUNTS=(${PREFIX_COUNTS:-10 100 1000 10000})
THREADS=(${THREADS:-1 8})
REPEATS=${REPEATS:-3}
OPS=${OPS:-500000}
WARMUP_OPS=${WARMUP_OPS:-10000}
READ_FRACTION=${READ_FRACTION:-0.95}
PREPOPULATE=${PREPOPULATE:-20000}
UNIVERSE=${UNIVERSE:-100000}
BASE_SEED=${BASE_SEED:-42}
TS=$(date +%Y%m%d_%H%M%S)
OUT=${OUT:-results/radix_sweep_${TS}.csv}
META=${META:-results/radix_sweep_${TS}.meta.json}
PROMOTE_AS_CANONICAL=${PROMOTE_AS_CANONICAL:-0}

mkdir -p results

GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "unversioned")
STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

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

  # Sweep 1: shared-prefix (sharing x threads) at read_frac=0.95.
  # Universe = prefix_count * key_space is held constant so hit rate is
  # stable across the sharing sweep and we're only varying tree shape.
  for t in "${TABLES[@]}"; do
    for pc in "${PREFIX_COUNTS[@]}"; do
      key_space=$((UNIVERSE / pc))
      for n in "${THREADS[@]}"; do
        run_one "$rep" --table="$t" --workload=shared-prefix \
          --prefix-count="$pc" --key-space="$key_space" \
          --threads="$n" --ops="$OPS" \
          --read-fraction="$READ_FRACTION" --prepopulate="$PREPOPULATE"
      done
    done
  done

  # Sweep 2: uniform baseline at 8 threads. Anchors the "hash wins on
  # random keys" half of the story.
  for t in "${TABLES[@]}"; do
    run_one "$rep" --table="$t" --workload=uniform --threads=8 \
      --ops="$OPS" --read-fraction="$READ_FRACTION" \
      --key-space="$UNIVERSE" --prepopulate="$PREPOPULATE"
  done
done

FINISHED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

if [[ "$PROMOTE_AS_CANONICAL" == "1" ]]; then
  CANONICAL_CSV=results/radix_sweep_canonical.csv
  CANONICAL_META=results/radix_sweep_canonical.meta.json
  mv "$OUT" "$CANONICAL_CSV"
  OUT="$CANONICAL_CSV"
  META="$CANONICAL_META"
fi

python3 - "$META" "$OUT" "$GIT_SHA" "$STARTED_AT" "$FINISHED_AT" \
  "$REPEATS" "$BASE_SEED" "$OPS" "$WARMUP_OPS" "$READ_FRACTION" \
  "$PREPOPULATE" "$UNIVERSE" \
  "${TABLES[*]}" "${PREFIX_COUNTS[*]}" "${THREADS[*]}" <<'PYEOF'
import json, sys
(meta_path, csv_path, git_sha, started_at, finished_at,
 repeats, base_seed, ops, warmup_ops, read_frac, prepop, universe,
 tables_str, prefix_counts_str, threads_str) = sys.argv[1:]
meta = {
  "csv": csv_path,
  "repeats": int(repeats), "base_seed": int(base_seed),
  "ops_per_run": int(ops), "warmup_ops_per_thread": int(warmup_ops),
  "read_fraction": float(read_frac), "prepopulate": int(prepop),
  "universe": int(universe),
  "git_sha": git_sha, "started_at": started_at, "finished_at": finished_at,
  "tables": tables_str.split(), "prefix_counts": prefix_counts_str.split(),
  "threads": threads_str.split(),
}
with open(meta_path, "w") as f:
  json.dump(meta, f, indent=2); f.write("\n")
PYEOF

echo "Wrote $OUT"
echo "Wrote $META"
