# Future Work

## 1. Real LLM Serving Trace (Highest Priority)

The current ShareGPT trace is derived offline: conversations are tokenized into 16-token blocks and converted into insert/lookup/remove operations, but there are no timestamps, no real inter-arrival times, and no genuine concurrency between requests. The benchmark artificially creates parallelism by having N threads replay the same flat operation sequence — which is not how real serving works.

**What to do:** Run LLaMA-3.2-1B through MLX (Apple's native M-series ML framework) on the M4 Pro, patch the KV cache to log every block allocation, lookup, and eviction with timestamps, then send ShareGPT conversations through it with realistic concurrency (multiple simultaneous requests). Replay the resulting log through the benchmark harness with real inter-arrival timing.

```bash
pip install mlx-lm
# patch mlx_lm/models/cache.py to log accesses
python replay_trace.py --conversations data/sharegpt52k.json \
  --concurrency 8 --out data/real_serving.trace
```

**What changes:** real prefix-sharing depth, real block sizes matching model architecture, real bursty arrival patterns, genuine concurrent cache access. The relative rankings between designs may shift — particularly for designs whose performance is contention-sensitive (hopscotch, cuckoo_striped).

---

## 2. Variable-Length Token Sequence Keys

All current implementations operate on `uint64_t` keys. Real LLM prefix caching operates on variable-length token sequences — blocks of 16 tokens represented as arrays of integers. A key might be `[12, 453, 78, 29, 1102, ...]`, not a single 64-bit integer.

This matters most for the radix tree extension: the memory and prefix-compression advantages of radix trees only materialize on long keys with shared prefixes. On uint64 keys with only 4 shared high-order bytes, an uncompressed trie is ~200× worse on memory than a hash table. On token-sequence keys of length 16+ with genuine prefix sharing across conversations, the radix tree reclaims its natural advantage.

**What to do:** extend the `HashTable` interface to support variable-length byte-array keys, or add a parallel `PrefixTable` interface. Re-implement radix tree variants for this key type.

---

## 3. Path Compression for Radix Trees

The current `RadixTreeCoarse` and `RadixTreeFine` implementations are uncompressed byte-level tries: each level corresponds to one byte of the key, giving a fixed 8-level tree for uint64 keys. Real radix trees collapse single-child chains into edge labels, which dramatically reduces node count and memory footprint on sparse key spaces.

Without path compression, the memory story for radix trees is inverted: the trie uses more memory than the hash table on uint64 keys with only top-level sharing. Path compression is what makes "radix tree" mean something beyond "k-ary trie."

**What to do:** implement a path-compressed radix tree where each node stores a variable-length edge label. Insertions split edges on divergence; lookups match edge labels before descending.

---

## 4. Lock-Free Radix Tree

The fine-grained radix tree (`RadixTreeFine`) uses hand-over-hand locking with `std::shared_mutex` per node. This gives ~3–4× scaling at 8 threads, limited by root-level serialization: every operation acquires the root lock first, so all threads briefly serialize there.

Published lock-free radix tree designs (Oshman & Shavit, PODC 2012; Brown et al.) use CAS-based node operations to avoid all per-node locks. Readers and writers proceed without blocking each other. This removes the root serialization ceiling entirely.

**What to do:** implement a lock-free radix tree using CAS on child pointers. Start with the Oshman & Shavit non-blocking design; validate with TSan (lock-free, unlike the optimistic hash tables, should pass TSan cleanly).

---

## 5. Adaptive Hybrid Design

No single data structure dominates across all workload regimes:

- Hash tables win on random-key workloads (no prefix sharing, low hit rate).
- Radix trees win on high-sharing workloads (many keys with long common prefixes).
- LLM serving traffic has both: cold-start periods with many unique keys and warm periods with heavy prefix reuse.

An adaptive system could monitor the observed prefix-sharing ratio at runtime and switch backing structures — or maintain both and route based on key structure. The measurement infrastructure to detect this already exists in the benchmark harness (`read_frac_measured`, `lookup_hits`).

**What to do:** implement an `AdaptiveTable` wrapper that tracks hit rate over a sliding window and migrates between a hash table and a radix tree when the sharing regime shifts. Measure migration overhead and end-to-end improvement on workloads that transition between phases.

---

## 6. SGLang C++ Backend Integration

SGLang's radix tree is implemented in Python. Under high concurrency, Python's GIL limits CPU utilization to approximately one core regardless of available hardware. This is a documented production bottleneck (SGLang issue #21061).

The `RadixTreeFine` implementation from this project is a direct C++ replacement candidate. Integrating it as a native extension (via pybind11 or a C API) would remove the GIL ceiling and allow the scheduler to utilize all available cores.

**What to do:** wrap `RadixTreeFine` with a pybind11 interface matching SGLang's internal radix tree API, integrate into SGLang's block manager, and measure end-to-end inference throughput improvement on concurrent request load.

---

## 7. Multi-Machine / Distributed KV Cache

All measurements in this project are single-machine, single-NUMA-domain. Production LLM serving increasingly disaggregates prefill and decode across separate machines (prefill servers handle dense prompt computation; decode servers handle token-by-token generation). This creates a distributed KV cache problem: blocks computed on the prefill server must be transferred to the decode server.

The data structure choice affects more than local throughput in this setting — it affects which keys are worth transferring, how to route requests for cache locality, and what consistency model is needed when multiple decode workers share a cache.

**What to do:** measure cache access patterns on a two-node prefill/decode split, evaluate which data structure designs are most network-transfer-friendly, and quantify the consistency tradeoff (strong vs. eventual) on hit rate and latency.
