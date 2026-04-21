#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <iostream>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "hash_table.hpp"
#include "histogram.hpp"
#include "thread_pool.hpp"
#include "timing.hpp"
#include "workload.hpp"

namespace ee361c {
namespace {

struct Config {
  std::string table;
  std::string workload = "uniform";
  std::string trace_path;
  size_t threads = 8;
  size_t ops = 10'000'000;
  double read_fraction = 0.9;
  double insert_fraction = 0.5;
  uint64_t key_space = 10'000'000;
  size_t initial_capacity = 16'000'000;
  size_t prepopulate_keys = 5'000'000;
  double zipf_alpha = 0.99;
  size_t warmup_ops = 100'000;
  uint64_t seed = 42;
  uint32_t prefix_count = 100;
  size_t latency_sample_rate = 1;
  size_t repeat = 0;
  std::string git_sha;
  bool csv = false;
  bool csv_header = false;
  bool verbose = false;
};

struct Slice {
  const Op* ops = nullptr;
  size_t len = 0;
};

struct RunStats {
  size_t measured_ops = 0;
  double wall_seconds = 0.0;
  Histogram merged_hist;
  uint64_t insert_attempts = 0;
  uint64_t insert_successes = 0;
  uint64_t remove_attempts = 0;
  uint64_t remove_successes = 0;
  uint64_t lookup_attempts = 0;
  uint64_t lookup_hits = 0;
};

void print_usage(const char* argv0) {
  std::cerr
      << "Usage: " << argv0 << " --table=NAME [options]\n"
      << "  --workload=uniform|zipfian|trace|shared-prefix\n"
      << "  --trace=PATH\n"
      << "  --threads=N\n"
      << "  --ops=N\n"
      << "  --read-fraction=F\n"
      << "  --insert-fraction=F\n"
      << "  --key-space=N\n"
      << "  --initial-capacity=N\n"
      << "  --prepopulate=N\n"
      << "  --zipf-alpha=F\n"
      << "  --prefix-count=N            (shared-prefix workload; default 100)\n"
      << "  --warmup-ops=N\n"
      << "  --seed=N\n"
      << "  --latency-sample-rate=N\n"
      << "  --repeat=N\n"
      << "  --git-sha=HASH\n"
      << "  --csv\n"
      << "  --csv-header\n"
      << "  --verbose\n";
}

bool parse_bool_flag(const std::string& arg, const std::string& name) {
  return arg == "--" + name;
}

std::string value_after_equals(const std::string& arg, const std::string& key) {
  const std::string prefix = "--" + key + "=";
  if (arg.rfind(prefix, 0) == 0) {
    return arg.substr(prefix.size());
  }
  return "";
}

template <typename T>
T parse_num(const std::string& s, const std::string& flag) {
  std::stringstream ss(s);
  T out{};
  ss >> out;
  if (!ss || !ss.eof()) {
    throw std::invalid_argument("invalid value for --" + flag + ": " + s);
  }
  return out;
}

Config parse_args(int argc, char** argv) {
  Config cfg;
  for (int i = 1; i < argc; ++i) {
    const std::string arg(argv[i]);
    if (parse_bool_flag(arg, "csv")) {
      cfg.csv = true;
      continue;
    }
    if (parse_bool_flag(arg, "csv-header")) {
      cfg.csv_header = true;
      continue;
    }
    if (parse_bool_flag(arg, "verbose")) {
      cfg.verbose = true;
      continue;
    }
    if (parse_bool_flag(arg, "help")) {
      print_usage(argv[0]);
      std::exit(0);
    }

    if (auto v = value_after_equals(arg, "table"); !v.empty()) {
      cfg.table = v;
    } else if (auto v = value_after_equals(arg, "workload"); !v.empty()) {
      cfg.workload = v;
    } else if (auto v = value_after_equals(arg, "trace"); !v.empty()) {
      cfg.trace_path = v;
    } else if (auto v = value_after_equals(arg, "threads"); !v.empty()) {
      cfg.threads = parse_num<size_t>(v, "threads");
    } else if (auto v = value_after_equals(arg, "ops"); !v.empty()) {
      cfg.ops = parse_num<size_t>(v, "ops");
    } else if (auto v = value_after_equals(arg, "read-fraction"); !v.empty()) {
      cfg.read_fraction = parse_num<double>(v, "read-fraction");
    } else if (auto v = value_after_equals(arg, "insert-fraction"); !v.empty()) {
      cfg.insert_fraction = parse_num<double>(v, "insert-fraction");
    } else if (auto v = value_after_equals(arg, "key-space"); !v.empty()) {
      cfg.key_space = parse_num<uint64_t>(v, "key-space");
    } else if (auto v = value_after_equals(arg, "initial-capacity"); !v.empty()) {
      cfg.initial_capacity = parse_num<size_t>(v, "initial-capacity");
    } else if (auto v = value_after_equals(arg, "prepopulate"); !v.empty()) {
      cfg.prepopulate_keys = parse_num<size_t>(v, "prepopulate");
    } else if (auto v = value_after_equals(arg, "zipf-alpha"); !v.empty()) {
      cfg.zipf_alpha = parse_num<double>(v, "zipf-alpha");
    } else if (auto v = value_after_equals(arg, "warmup-ops"); !v.empty()) {
      cfg.warmup_ops = parse_num<size_t>(v, "warmup-ops");
    } else if (auto v = value_after_equals(arg, "seed"); !v.empty()) {
      cfg.seed = parse_num<uint64_t>(v, "seed");
    } else if (auto v = value_after_equals(arg, "prefix-count"); !v.empty()) {
      cfg.prefix_count = parse_num<uint32_t>(v, "prefix-count");
    } else if (auto v = value_after_equals(arg, "latency-sample-rate"); !v.empty()) {
      cfg.latency_sample_rate = parse_num<size_t>(v, "latency-sample-rate");
    } else if (auto v = value_after_equals(arg, "repeat"); !v.empty()) {
      cfg.repeat = parse_num<size_t>(v, "repeat");
    } else if (auto v = value_after_equals(arg, "git-sha"); !v.empty()) {
      cfg.git_sha = v;
    } else {
      throw std::invalid_argument("unknown flag: " + arg);
    }
  }

  if (cfg.table.empty()) {
    throw std::invalid_argument("--table is required");
  }
  const std::set<std::string> valid_workloads = {"uniform", "zipfian", "trace",
                                                  "shared-prefix"};
  if (valid_workloads.find(cfg.workload) == valid_workloads.end()) {
    throw std::invalid_argument("--workload must be one of: uniform, zipfian, trace");
  }
  if (cfg.threads == 0) {
    throw std::invalid_argument("--threads must be > 0");
  }
  if (cfg.read_fraction < 0.0 || cfg.read_fraction > 1.0) {
    throw std::invalid_argument("--read-fraction must be in [0,1]");
  }
  if (cfg.insert_fraction < 0.0 || cfg.insert_fraction > 1.0) {
    throw std::invalid_argument("--insert-fraction must be in [0,1]");
  }
  if (cfg.latency_sample_rate == 0) {
    throw std::invalid_argument("--latency-sample-rate must be > 0");
  }
  if (cfg.workload == "trace" && cfg.trace_path.empty()) {
    throw std::invalid_argument("--trace is required when --workload=trace");
  }
  if (cfg.workload == "shared-prefix" && cfg.prefix_count == 0) {
    throw std::invalid_argument("--prefix-count must be > 0 for shared-prefix");
  }
  return cfg;
}

std::vector<Slice> make_slices(const Workload& workload, size_t threads) {
  std::vector<Slice> slices(threads);
  const size_t total = workload.size();
  const size_t base = total / threads;
  const size_t rem = total % threads;
  size_t offset = 0;
  for (size_t t = 0; t < threads; ++t) {
    const size_t len = base + (t < rem ? 1 : 0);
    slices[t] = Slice{workload.data() + offset, len};
    offset += len;
  }
  return slices;
}

struct ThreadStats {
  uint64_t insert_attempts = 0;
  uint64_t insert_successes = 0;
  uint64_t remove_attempts = 0;
  uint64_t remove_successes = 0;
  uint64_t lookup_attempts = 0;
  uint64_t lookup_hits = 0;
};

void run_slice(HashTable& table,
               const Op* ops,
               size_t n,
               Histogram& hist,
               size_t sample_rate,
               ThreadStats& stats) {
  Value out = kNoValue;
  for (size_t i = 0; i < n; ++i) {
    const bool sample = (i % sample_rate) == 0;
    uint64_t t0 = 0;
    if (sample) {
      t0 = sample_ns();
    }

    switch (ops[i].type) {
      case OpType::kInsert: {
        const bool ok = table.insert(ops[i].key, ops[i].value);
        ++stats.insert_attempts;
        if (ok) {
          ++stats.insert_successes;
        }
        break;
      }
      case OpType::kLookup: {
        const bool ok = table.lookup(ops[i].key, out);
        ++stats.lookup_attempts;
        if (ok) {
          ++stats.lookup_hits;
        }
        break;
      }
      case OpType::kRemove: {
        const bool ok = table.remove(ops[i].key);
        ++stats.remove_attempts;
        if (ok) {
          ++stats.remove_successes;
        }
        break;
      }
    }

    if (sample) {
      const uint64_t t1 = sample_ns();
      hist.record(t1 - t0);
    }
  }
}

RunStats execute(HashTable& table,
                 const std::vector<Slice>& slices,
                 size_t warmup_ops,
                 size_t sample_rate,
                 size_t threads) {
  ThreadPool pool(threads);
  std::vector<Histogram> per_thread(threads);
  std::vector<ThreadStats> per_thread_stats(threads);
  std::vector<ThreadStats> warmup_stats(threads);

  // Warmup runs the prefix of each slice; measurement advances past that prefix
  // so we never replay warmup ops during the timed window (a replay would
  // inflate hit rates and make inserts return false).
  std::vector<size_t> warmup_lens(threads);
  for (size_t t = 0; t < threads; ++t) {
    warmup_lens[t] = std::min(warmup_ops, slices[t].len);
  }

  size_t measured_ops = 0;
  for (size_t t = 0; t < threads; ++t) {
    measured_ops += slices[t].len - warmup_lens[t];
  }

  pool.run([&](size_t tid) {
    const Slice& s = slices[tid];
    run_slice(table, s.ops, warmup_lens[tid], per_thread[tid], sample_rate,
              warmup_stats[tid]);
  });

  for (auto& h : per_thread) {
    h = Histogram{};
  }

  Timer timer;
  pool.run([&](size_t tid) {
    const Slice& s = slices[tid];
    const size_t offset = warmup_lens[tid];
    run_slice(table, s.ops + offset, s.len - offset, per_thread[tid],
              sample_rate, per_thread_stats[tid]);
  });
  const double wall = timer.elapsed_seconds();

  RunStats result;
  result.measured_ops = measured_ops;
  result.wall_seconds = wall;
  for (const auto& h : per_thread) {
    result.merged_hist.merge(h);
  }
  for (const auto& s : per_thread_stats) {
    result.insert_attempts += s.insert_attempts;
    result.insert_successes += s.insert_successes;
    result.remove_attempts += s.remove_attempts;
    result.remove_successes += s.remove_successes;
    result.lookup_attempts += s.lookup_attempts;
    result.lookup_hits += s.lookup_hits;
  }
  return result;
}

double measured_read_fraction(const RunStats& stats) {
  const uint64_t total =
      stats.insert_attempts + stats.remove_attempts + stats.lookup_attempts;
  if (total == 0) {
    return 0.0;
  }
  return static_cast<double>(stats.lookup_attempts) / static_cast<double>(total);
}

void print_csv(const Config& cfg, const RunStats& stats) {
  if (cfg.csv_header) {
    std::cout
        << "table,workload,threads,ops,"
        << "read_frac_config,read_frac_measured,key_space,"
        << "throughput_mops,p50_ns,p99_ns,p999_ns,max_ns,"
        << "insert_attempts,insert_successes,"
        << "remove_attempts,remove_successes,"
        << "lookup_attempts,lookup_hits,"
        << "wall_seconds,seed,repeat,git_sha\n";
  }

  const double throughput_mops =
      stats.wall_seconds > 0.0
          ? (static_cast<double>(stats.measured_ops) / stats.wall_seconds) / 1e6
          : 0.0;
  const double measured_rf = measured_read_fraction(stats);

  std::ostringstream line;
  line << cfg.table << "," << cfg.workload << "," << cfg.threads << ","
       << stats.measured_ops << "," << std::fixed << std::setprecision(6)
       << cfg.read_fraction << "," << std::fixed << std::setprecision(6)
       << measured_rf << "," << cfg.key_space << "," << std::fixed
       << std::setprecision(6) << throughput_mops << ","
       << stats.merged_hist.p50() << "," << stats.merged_hist.p99() << ","
       << stats.merged_hist.p999() << "," << stats.merged_hist.max() << ","
       << stats.insert_attempts << "," << stats.insert_successes << ","
       << stats.remove_attempts << "," << stats.remove_successes << ","
       << stats.lookup_attempts << "," << stats.lookup_hits << ","
       << std::fixed << std::setprecision(6) << stats.wall_seconds << ","
       << cfg.seed << "," << cfg.repeat << "," << cfg.git_sha;
  std::cout << line.str() << "\n";
}

void print_verbose(const Config& cfg, const RunStats& stats) {
  const double throughput_mops =
      stats.wall_seconds > 0.0
          ? (static_cast<double>(stats.measured_ops) / stats.wall_seconds) / 1e6
          : 0.0;

  const double non_read = 1.0 - cfg.read_fraction;
  const double insert_frac = non_read * cfg.insert_fraction;
  const double remove_frac = non_read * (1.0 - cfg.insert_fraction);

  std::cout << "=== ee361c benchmark ===\n";
  std::cout << "  table:         " << cfg.table << "\n";
  std::cout << "  workload:      " << cfg.workload << "  (" << (cfg.read_fraction * 100.0)
            << "% read, " << (insert_frac * 100.0) << "% insert, " << (remove_frac * 100.0)
            << "% remove)\n";
  if (cfg.workload == "trace") {
    std::cout << "  measured mix:  "
              << (measured_read_fraction(stats) * 100.0) << "% read\n";
  }
  std::cout << "  threads:       " << cfg.threads << "\n";
  std::cout << "  ops:           " << stats.measured_ops << "\n";
  std::cout << "  key space:     " << cfg.key_space << "\n";
  std::cout << "  prepopulated:  " << cfg.prepopulate_keys << " keys\n";
  std::cout << "Measurement:\n";
  std::cout << "  Wall time:     " << std::fixed << std::setprecision(6) << stats.wall_seconds
            << " s\n";
  std::cout << "  Throughput:    " << std::fixed << std::setprecision(3) << throughput_mops
            << " Mops/s\n";
  std::cout << "  Latency p50:   " << stats.merged_hist.p50() << " ns\n";
  std::cout << "  Latency p99:   " << stats.merged_hist.p99() << " ns\n";
  std::cout << "  Latency p999:  " << stats.merged_hist.p999() << " ns\n";
  std::cout << "  Latency max:   " << stats.merged_hist.max() << " ns\n";
  std::cout << "  Insert:        " << stats.insert_successes << "/"
            << stats.insert_attempts << " succeeded\n";
  std::cout << "  Remove:        " << stats.remove_successes << "/"
            << stats.remove_attempts << " succeeded\n";
  std::cout << "  Lookup hits:   " << stats.lookup_hits << "/"
            << stats.lookup_attempts << "\n";
}

}  // namespace
}  // namespace ee361c

int main(int argc, char** argv) {
  try {
    const ee361c::Config cfg = ee361c::parse_args(argc, argv);
    auto table = ee361c::make_by_name(cfg.table, cfg.initial_capacity);
    if (!table) {
      throw std::runtime_error("unknown or unavailable table: " + cfg.table);
    }

    {
      std::vector<ee361c::Key> inserted;
      if (cfg.workload == "shared-prefix") {
        inserted = ee361c::prepopulate_shared_prefix(
            *table, cfg.prepopulate_keys, cfg.prefix_count, cfg.key_space,
            cfg.seed ^ 0xA11CE);
      } else {
        inserted = ee361c::prepopulate(
            *table, cfg.prepopulate_keys, cfg.key_space, cfg.seed ^ 0xA11CE);
      }
      if (inserted.size() < cfg.prepopulate_keys) {
        std::cerr << "warning: prepopulate landed " << inserted.size()
                  << " / " << cfg.prepopulate_keys
                  << " keys (table refused further inserts; "
                  << "treat as fill-cap for this table)\n";
      }
    }

    std::unique_ptr<ee361c::Workload> workload;
    ee361c::WorkloadConfig wcfg;
    wcfg.num_ops = cfg.ops;
    wcfg.read_fraction = cfg.read_fraction;
    wcfg.insert_fraction = cfg.insert_fraction;
    wcfg.key_space = cfg.key_space;
    wcfg.seed = cfg.seed;
    wcfg.prefix_count = cfg.prefix_count;

    if (cfg.workload == "uniform") {
      workload = ee361c::generate_uniform(wcfg);
    } else if (cfg.workload == "zipfian") {
      workload = ee361c::generate_zipfian(wcfg, cfg.zipf_alpha);
    } else if (cfg.workload == "shared-prefix") {
      workload = ee361c::generate_shared_prefix(wcfg);
    } else if (cfg.workload == "trace") {
      workload = ee361c::load_trace(cfg.trace_path);
    } else {
      throw std::invalid_argument("unsupported workload: " + cfg.workload);
    }

    const auto slices = ee361c::make_slices(*workload, cfg.threads);
    const ee361c::RunStats stats =
        ee361c::execute(*table, slices, cfg.warmup_ops, cfg.latency_sample_rate, cfg.threads);

    if (stats.measured_ops == 0) {
      std::cerr << "warning: measured_ops is zero; warmup consumed the entire "
                << "per-thread slice. Reduce --warmup-ops or increase --ops "
                << "(currently warmup_ops=" << cfg.warmup_ops
                << ", total ops=" << cfg.ops
                << ", threads=" << cfg.threads << ").\n";
    }

    if (cfg.csv) {
      ee361c::print_csv(cfg, stats);
    } else {
      ee361c::print_verbose(cfg, stats);
    }
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "bench error: " << ex.what() << "\n";
    return 1;
  }
}
