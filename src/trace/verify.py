#!/usr/bin/env python3

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path

from trace_format import (
    OP_INSERT, OP_LOOKUP, OP_REMOVE, content_sha256, iter_trace, read_header,
)


def sha256_file(path: Path) -> str:
  h = hashlib.sha256()
  with path.open("rb") as f:
    for chunk in iter(lambda: f.read(1 << 20), b""):
      h.update(chunk)
  return h.hexdigest()


def _looks_like_path(value: str) -> bool:
  # A hex digest has a fixed set of characters and never contains these;
  # treat anything that does as "intended to be a path" so a typo doesn't
  # silently fall through to mismatch.
  if "/" in value or "\\" in value:
    return True
  lowered = value.lower()
  return lowered.endswith((".sha256", ".json", ".txt"))


def resolve_expected_sha(value: str) -> str:
  """Accept a raw hex digest, a path to a file containing one, or a JSON file
  with a top-level "trace_sha256" field (e.g. experiments_canonical.meta.json).

  If the argument *looks* like a path but doesn't resolve, exit with a clear
  not-found error rather than silently treating it as a hex digest (which
  would otherwise surface later as a confusing "sha256 mismatch")."""
  p = Path(value)
  if p.is_file():
    content = p.read_text(encoding="utf-8").strip()
    if content.startswith("{"):
      data = json.loads(content)
      sha = data.get("trace_sha256", "")
      if not sha:
        raise SystemExit(f"{p}: no 'trace_sha256' field")
      return sha.lower()
    # Plain checksum file: either "<hex>" or "<hex>  <path>" (shasum format).
    token = content.split()[0] if content else ""
    if not token:
      raise SystemExit(f"{p}: empty checksum file")
    return token.lower()
  if _looks_like_path(value):
    raise SystemExit(f"--verify-sha: expected-sha source not found: {value}")
  return value.strip().lower()


def main() -> int:
  parser = argparse.ArgumentParser(description="Verify LLMT trace file sanity.")
  parser.add_argument("trace", type=Path)
  parser.add_argument("--min-read-frac", type=float, default=0.60)
  parser.add_argument("--max-read-frac", type=float, default=0.95)
  parser.add_argument(
      "--verify-sha",
      default=None,
      help="Expected SHA256: raw hex, a file containing it, or a JSON meta "
           "file with a 'trace_sha256' field. Exits non-zero on mismatch.")
  args = parser.parse_args()

  header = read_header(args.trace)

  op_counts = Counter()
  unique_keys = set()
  total_ops = 0
  for op_type, key, _value in iter_trace(args.trace):
    op_counts[op_type] += 1
    unique_keys.add(key)
    total_ops += 1

  if total_ops == 0:
    raise SystemExit("trace verification failed: no operations found")

  read_frac = op_counts[OP_LOOKUP] / total_ops
  if not (args.min_read_frac <= read_frac <= args.max_read_frac):
    raise SystemExit(
        f"trace verification failed: read fraction {read_frac:.4f} outside "
        f"[{args.min_read_frac:.2f}, {args.max_read_frac:.2f}]")

  if len(unique_keys) >= total_ops:
    raise SystemExit(
        "trace verification failed: unique key count should be lower than op count")

  observed_file_sha = sha256_file(args.trace)
  observed_content_sha = content_sha256(args.trace)

  print(f"trace: {args.trace}")
  print(f"version: {header['version']}")
  print(f"total_ops: {total_ops}")
  print(f"insert_ops: {op_counts[OP_INSERT]}")
  print(f"lookup_ops: {op_counts[OP_LOOKUP]}")
  print(f"remove_ops: {op_counts[OP_REMOVE]}")
  print(f"read_fraction: {read_frac:.4f}")
  print(f"unique_keys: {len(unique_keys)}")
  # Two hashes: file-level (includes timestamped header, so drifts across
  # regenerations) and content-level (over just the records, stable across
  # regenerations of the same data). The committed .sha256 and the meta JSON
  # both store the content hash so regeneration stays verifiable.
  print(f"file_sha256: {observed_file_sha}")
  print(f"content_sha256: {observed_content_sha}")
  print("provenance:")
  print(json.dumps(header["provenance"], indent=2, sort_keys=True))

  if args.verify_sha is not None:
    expected = resolve_expected_sha(args.verify_sha)
    if observed_content_sha != expected:
      raise SystemExit(
          f"content_sha256 mismatch: observed {observed_content_sha}, "
          f"expected {expected}")
    print("content_sha256 matches expected.")
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
