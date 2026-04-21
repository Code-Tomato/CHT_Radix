#!/usr/bin/env python3

import argparse
import datetime as _dt
import hashlib
import random
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from trace_format import OP_INSERT, OP_LOOKUP, OP_REMOVE, write_trace


def block_key(tokens: Sequence[int]) -> int:
  payload = ",".join(str(x) for x in tokens).encode("utf-8")
  return int.from_bytes(hashlib.blake2b(payload, digest_size=8).digest(), "little")


def iter_blocks_from_text(text: str, encoder, block_size: int) -> Iterator[Tuple[int, ...]]:
  token_ids = encoder.encode(text)
  for i in range(0, len(token_ids), block_size):
    chunk = token_ids[i:i + block_size]
    if len(chunk) < block_size:
      continue
    yield tuple(chunk)


def update_lru_and_emit(
    key: int,
    next_value: int,
    live: OrderedDict,
    lru_cap: int,
    ops: List[Tuple[int, int, int]],
    replay_on_hit: int,
) -> int:
  ops.append((OP_LOOKUP, key, 0))
  if key not in live:
    ops.append((OP_INSERT, key, next_value))
    live[key] = next_value
    next_value += 1
  else:
    for _ in range(replay_on_hit):
      ops.append((OP_LOOKUP, key, 0))
    value = live.pop(key)
    live[key] = value

  while len(live) > lru_cap:
    old_key, _old_value = live.popitem(last=False)
    ops.append((OP_REMOVE, old_key, 0))
  return next_value


def extract_turn_texts(example: Dict) -> List[str]:
  texts: List[str] = []
  if "conversations" in example and isinstance(example["conversations"], list):
    for turn in example["conversations"]:
      if isinstance(turn, dict):
        value = turn.get("value") or turn.get("text") or turn.get("content")
        if isinstance(value, str) and value.strip():
          texts.append(value)
  elif "messages" in example and isinstance(example["messages"], list):
    for msg in example["messages"]:
      if isinstance(msg, dict):
        value = msg.get("content") or msg.get("text")
        if isinstance(value, str) and value.strip():
          texts.append(value)
  elif "text" in example and isinstance(example["text"], str):
    if example["text"].strip():
      texts.append(example["text"])
  return texts


def build_sharegpt_ops(
    max_conversations: int,
    max_ops: int,
    block_size: int,
    lru_cap: int,
    timebox_seconds: int,
    replay_on_hit: int,
    out_info: Dict[str, Any],
) -> List[Tuple[int, int, int]]:
  import tiktoken
  from datasets import load_dataset

  dataset_candidates = [
      "RyokoAI/ShareGPT52K",
      "anon8231489123/ShareGPT_Vicuna_unfiltered",
  ]
  dataset = None
  loaded = None
  for candidate in dataset_candidates:
    try:
      dataset = load_dataset(candidate, split="train")
      loaded = candidate
      print(f"Loaded dataset: {candidate}")
      break
    except Exception as ex:
      print(f"Failed dataset {candidate}: {ex}")
  if dataset is None:
    raise RuntimeError("Unable to load any ShareGPT dataset candidate")
  out_info["source"] = loaded

  encoder = tiktoken.get_encoding("cl100k_base")
  live: OrderedDict[int, int] = OrderedDict()
  ops: List[Tuple[int, int, int]] = []
  next_value = 1
  start = time.time()
  conversations = 0

  for example in dataset:
    if conversations >= max_conversations or len(ops) >= max_ops:
      break
    if time.time() - start > timebox_seconds:
      raise TimeoutError(
          f"ShareGPT extraction exceeded {timebox_seconds}s timebox")
    turns = extract_turn_texts(example)
    if not turns:
      continue
    conversations += 1
    for text in turns:
      for block in iter_blocks_from_text(text, encoder, block_size):
        key = block_key(block)
        next_value = update_lru_and_emit(
            key, next_value, live, lru_cap, ops, replay_on_hit)
        if len(ops) >= max_ops:
          break
      if len(ops) >= max_ops:
        break

  print(f"Processed conversations: {conversations}")
  return ops[:max_ops]


def zipf_sample(rng: random.Random, n: int, alpha: float) -> int:
  # Simple inverse-CDF over normalized finite Zipf support.
  # This is offline trace generation, so O(n) prep + O(log n) sample is fine.
  if not hasattr(zipf_sample, "_cache"):
    zipf_sample._cache = {}
  cache = zipf_sample._cache
  key = (n, alpha)
  if key not in cache:
    weights = [1.0 / ((i + 1) ** alpha) for i in range(n)]
    total = sum(weights)
    cdf = []
    run = 0.0
    for w in weights:
      run += w / total
      cdf.append(run)
    cache[key] = cdf
  cdf = cache[key]
  x = rng.random()
  lo, hi = 0, len(cdf) - 1
  while lo < hi:
    mid = (lo + hi) // 2
    if cdf[mid] < x:
      lo = mid + 1
    else:
      hi = mid
  return lo


def build_synthetic_ops(
    max_ops: int,
    lru_cap: int,
    hot_set: int = 1000,
    cold_pool: int = 2_000_000,
    alpha: float = 0.99,
    replay_on_hit: int = 1,
) -> List[Tuple[int, int, int]]:
  rng = random.Random(42)
  live: OrderedDict[int, int] = OrderedDict()
  ops: List[Tuple[int, int, int]] = []
  next_value = 1
  cold_next = 10_000_000

  while len(ops) < max_ops:
    if rng.random() < 0.8:
      hot_idx = zipf_sample(rng, hot_set, alpha)
      key = hot_idx + 1
    else:
      key = cold_next
      cold_next += 1
      if cold_next >= 10_000_000 + cold_pool:
        cold_next = 10_000_000
    next_value = update_lru_and_emit(key, next_value, live, lru_cap, ops, replay_on_hit)
  return ops[:max_ops]


def main() -> int:
  parser = argparse.ArgumentParser(description="Generate LLM-like trace for the benchmark.")
  parser.add_argument("--out", type=Path, default=Path("data/sharegpt.trace"))
  parser.add_argument("--mode", choices=["sharegpt", "synthetic"], default="sharegpt")
  parser.add_argument("--max-conversations", type=int, default=50_000)
  parser.add_argument("--max-ops", type=int, default=20_000_000)
  parser.add_argument("--block-size", type=int, default=16)
  parser.add_argument("--lru-cap", type=int, default=1_000_000)
  parser.add_argument("--timebox-seconds", type=int, default=1200)
  parser.add_argument("--replay-on-hit", type=int, default=1)
  parser.add_argument("--allow-fallback", action="store_true", default=True)
  args = parser.parse_args()

  provenance: Dict[str, Any] = {
      "mode": args.mode,
      "source": None,
      "block_size": args.block_size,
      "lru_cap": args.lru_cap,
      "replay_on_hit": args.replay_on_hit,
      "tiktoken_encoding": "cl100k_base" if args.mode == "sharegpt" else None,
      "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
      "max_ops_requested": args.max_ops,
      "max_conversations": args.max_conversations,
  }

  try:
    if args.mode == "sharegpt":
      ops = build_sharegpt_ops(
          max_conversations=args.max_conversations,
          max_ops=args.max_ops,
          block_size=args.block_size,
          lru_cap=args.lru_cap,
          timebox_seconds=args.timebox_seconds,
          replay_on_hit=args.replay_on_hit,
          out_info=provenance)
    else:
      provenance["source"] = "synthetic"
      ops = build_synthetic_ops(
          max_ops=args.max_ops, lru_cap=args.lru_cap, replay_on_hit=args.replay_on_hit)
  except Exception as ex:
    if not args.allow_fallback:
      raise
    print(f"ShareGPT generation failed ({ex}); falling back to synthetic mode.")
    provenance["mode"] = "synthetic"
    provenance["source"] = "synthetic"
    provenance["fallback_reason"] = str(ex)
    provenance["tiktoken_encoding"] = None
    ops = build_synthetic_ops(
        max_ops=args.max_ops, lru_cap=args.lru_cap, replay_on_hit=args.replay_on_hit)

  count = write_trace(args.out, ops, provenance=provenance)
  print(f"Wrote {count} ops to {args.out}")
  print(f"Provenance: {provenance}")
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
