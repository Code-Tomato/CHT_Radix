<h1 align="center">CHT-Radix</h1>

<p align="center"><em>Concurrent hash tables and radix trees, benchmarked under synthetic and LLM-derived cache workloads.</em></p>

<p align="center">
  <a href="paper/CHT_Radix.pdf">Paper</a> |
  <a href="REPRODUCIBILITY.md">Reproducibility</a> |
  <a href="https://code-tomato.github.io/CHT_Radix/">Architecture &amp; results</a> |
  <a href="CITATION.cff">Citation</a> |
  <a href="LICENSE">License</a>
</p>

<p align="center">
  <a href="LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/License-MIT-yellow.svg"></a>
  <a href="CMakeLists.txt"><img alt="C++17" src="https://img.shields.io/badge/C%2B%2B-17-blue.svg"></a>
  <a href="requirements.txt"><img alt="Python 3.9+" src="https://img.shields.io/badge/Python-3.9%2B-blue.svg"></a>
  <a href="REPRODUCIBILITY.md"><img alt="Reproducible" src="https://img.shields.io/badge/Results-Reproducible-success.svg"></a>
</p>

<p align="center"><em>EE 361C (Multicore Algorithms), The University of Texas at Austin</em></p>

---

## About

CHT-Radix is a C++17 benchmarking suite that puts concurrent hash tables and concurrent radix trees on the same footing, then measures them on both classic synthetic traffic and an LLM-inspired prefix-caching trace. Most data-structure benchmarks use synthetic workloads only; this project asks whether rankings change when the workload starts to look like real LLM prefix caches, and how hash tables compare to radix trees under prefix-sharing patterns. The methods and results are summarized in [**paper/CHT_Radix.pdf**](paper/CHT_Radix.pdf).

CHT-Radix ships with:

- Five concurrent hash-table variants behind a shared `HashTable` interface (`insert`, `lookup`, `remove`)
- Two concurrent radix-tree variants for prefix-friendly workloads
- A benchmark harness that reports throughput and tail-latency metrics
- Reproducible experiment scripts, canonical CSVs, and plotting utilities
- A PDF write-up ([**CHT_Radix.pdf**](paper/CHT_Radix.pdf)) and an [interactive architecture explorer](https://code-tomato.github.io/CHT_Radix/) (same app as [docs/index.html](docs/index.html); layouts, structure comparison, and result summaries)

CHT-Radix is designed for measurement clarity:

- **Hash tables:** `chaining_coarse` (one global lock), `chaining_fine` (one lock per bucket), `cuckoo_optimistic` (optimistic readers, locked writers), `cuckoo_striped` (striped locks with displacement paths), `hopscotch` (hopscotch hashing with optimistic reads)
- **Radix trees:** `radix_tree_coarse` (256-way trie, one global lock), `radix_tree_fine` (atomic pointer walk, lock only at leaf value fields)
- **Workloads:** uniform, skewed, cache-resident, read-heavy, and an LLM-style trace replayed from ShareGPT prefixes
- **Metrics:** throughput vs. threads, p99 latency vs. threads, throughput vs. read fraction, radix scaling and prefix-count sweeps

### Architecture and results

**Start at the [live architecture explorer](https://code-tomato.github.io/CHT_Radix/)** (or open [`docs/index.html`](docs/index.html) locally) — it combines structure diagrams, variant comparison, and navigation to the main result plots. For static figures and raw numbers, the committed artifacts live under [`results/`](results/):

- [throughput_vs_threads.pdf](results/throughput_vs_threads.pdf)
- [p99_vs_threads.pdf](results/p99_vs_threads.pdf)
- [throughput_vs_read_fraction.pdf](results/throughput_vs_read_fraction.pdf)
- [throughput_cache_resident.pdf](results/throughput_cache_resident.pdf)
- [radix_scaling_1t_8t.pdf](results/radix_scaling_1t_8t.pdf)
- [radix_throughput_vs_prefix_count.pdf](results/radix_throughput_vs_prefix_count.pdf)

Canonical dataset: [results/experiments_canonical.csv](results/experiments_canonical.csv), [results/experiments_canonical.meta.json](results/experiments_canonical.meta.json).

### Repository layout

```text
include/      shared interfaces and helpers
src/tables/   data-structure implementations
src/bench/    benchmark harness + workloads
src/tests/    correctness tests
src/trace/    trace generation + format tools
scripts/      experiment sweeps and plotting
results/      committed canonical results (PDFs + CSVs)
paper/        CHT_Radix.pdf (write-up)
docs/         index.html (GitHub Pages) and notes (e.g. future_work.md)
```

### Limitations

- Fixed-size tables (no dynamic resizing)
- Single-machine evaluation
- Radix trees are uncompressed byte-level tries in this version
- `radix_tree_fine` still uses a leaf-level mutex for value fields

## Getting Started

Build and run the correctness tests and a single benchmark:

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j

./build/correctness_test --threads=16 --repeats=3 \
  --tables=stub,chaining_coarse,chaining_fine,cuckoo_optimistic,cuckoo_striped,hopscotch,radix_tree_coarse,radix_tree_fine

./build/bench --table=cuckoo_optimistic --workload=uniform --threads=8 --ops=5000000
```

### LLM-style trace workload

```bash
python3 -m pip install -r requirements.txt
python3 src/trace/extract.py --out data/sharegpt.trace \
  --max-ops 1000000 --replay-on-hit 2 --lru-cap 320000

./build/bench --table=chaining_fine --workload=trace \
  --trace=data/sharegpt.trace --threads=8
```

### Requirements

- C++17 compiler
- CMake 3.15+
- Python 3.9+ for trace extraction and plotting

### Site (GitHub Pages)

The interactive explorer is [`docs/index.html`](docs/index.html) and, once enabled, the project website is **[https://code-tomato.github.io/CHT_Radix/](https://code-tomato.github.io/CHT_Radix/)** (the same page as the repo root of the static site). In the GitHub repo, open **Settings → Pages**, set **Source** to **Deploy from a branch**, **Branch** `main`, **Folder** `/docs`, then save. After the workflow runs, the URL above serves the explorer as the site homepage.

Useful next steps:

- [**paper/CHT_Radix.pdf**](paper/CHT_Radix.pdf) — read the full evaluation
- [REPRODUCIBILITY.md](REPRODUCIBILITY.md) — full rerun recipe
- [https://code-tomato.github.io/CHT_Radix/](https://code-tomato.github.io/CHT_Radix/) — live architecture & results (GitHub Pages)
- [docs/index.html](docs/index.html) — same page, in-repo
- [requirements.txt](requirements.txt) — Python dependencies

## Contributing

This repository is primarily a course artifact, but issues and pull requests that improve correctness tests, benchmark fidelity, or reproducibility are welcome at the [GitHub repository](https://github.com/Code-Tomato/CHT_Radix).

## Citation

If CHT-Radix is useful for your work, please cite it:

```bibtex
@misc{lemma2026chtradix,
  title        = {Concurrent Hash Tables and Radix Trees Under LLM-Derived Workloads},
  author       = {Lemma, Nathan and Chikosha, Joshua and Teshome, Yonatan},
  year         = {2026},
  institution  = {The University of Texas at Austin},
  howpublished = {Course project, EE 361C (Multicore Algorithms)},
  note         = {Preprint; see CITATION.cff for canonical metadata}
}
```

Canonical metadata is tracked in [CITATION.cff](CITATION.cff).

## Contact

- For bugs, questions, or feature requests, please use GitHub [Issues](https://github.com/Code-Tomato/CHT_Radix/issues)
- For the write-up, open [**paper/CHT_Radix.pdf**](paper/CHT_Radix.pdf)

## License

CHT-Radix is released under the MIT License — see [LICENSE](LICENSE).
