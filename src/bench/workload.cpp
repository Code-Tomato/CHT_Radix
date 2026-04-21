#include "workload.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <random>
#include <stdexcept>
#include <unordered_set>

namespace ee361c {

namespace {

constexpr uint32_t kTraceMagic = 0x4C4C4D54U;
constexpr uint32_t kTraceVersionV1 = 1U;
constexpr uint32_t kTraceVersionV2 = 2U;

class ZipfGenerator {
public:
  ZipfGenerator(uint64_t n, double theta, uint64_t seed)
      : n_(std::max<uint64_t>(1, n)), theta_(theta), rng_(seed), dist_(0.0, 1.0) {
    if (theta_ <= 0.0 || theta_ >= 1.0) {
      throw std::invalid_argument("zipf alpha must be in (0, 1)");
    }
    zeta_n_ = zeta(n_, theta_);
    zeta_2_ = zeta(2, theta_);
    alpha_ = 1.0 / (1.0 - theta_);
    eta_ = (1.0 - std::pow(2.0 / static_cast<double>(n_), 1.0 - theta_)) /
           (1.0 - (zeta_2_ / zeta_n_));
  }

  uint64_t sample() {
    const double u = dist_(rng_);
    const double uz = u * zeta_n_;
    if (uz < 1.0) {
      return 0;
    }
    if (uz < 1.0 + std::pow(0.5, theta_)) {
      return std::min<uint64_t>(1, n_ - 1);
    }
    const double val =
        static_cast<double>(n_) * std::pow(eta_ * u - eta_ + 1.0, alpha_);
    const uint64_t idx = static_cast<uint64_t>(val);
    return std::min<uint64_t>(idx, n_ - 1);
  }

private:
  static double zeta(uint64_t n, double theta) {
    double sum = 0.0;
    for (uint64_t i = 1; i <= n; ++i) {
      sum += 1.0 / std::pow(static_cast<double>(i), theta);
    }
    return sum;
  }

  uint64_t n_;
  double theta_;
  std::mt19937_64 rng_;
  std::uniform_real_distribution<double> dist_;
  double zeta_n_ = 0.0;
  double zeta_2_ = 0.0;
  double alpha_ = 0.0;
  double eta_ = 0.0;
};

OpType pick_op_type(double u, const WorkloadConfig& cfg) {
  const double read_cutoff = cfg.read_fraction;
  const double insert_cutoff =
      read_cutoff + (1.0 - cfg.read_fraction) * cfg.insert_fraction;
  if (u < read_cutoff) {
    return OpType::kLookup;
  }
  if (u < insert_cutoff) {
    return OpType::kInsert;
  }
  return OpType::kRemove;
}

inline Key make_shared_prefix_key(std::mt19937_64& rng, uint32_t num_prefixes,
                                  uint64_t suffix_space) {
  const uint32_t prefix_id = static_cast<uint32_t>(rng() % num_prefixes);
  const uint32_t suffix = static_cast<uint32_t>(rng() % suffix_space);
  return (static_cast<Key>(prefix_id) << 32) | static_cast<Key>(suffix);
}

}  // namespace

std::unique_ptr<Workload> generate_uniform(const WorkloadConfig& cfg) {
  if (cfg.key_space == 0) {
    throw std::invalid_argument("key_space must be > 0");
  }
  auto workload = std::make_unique<Workload>(cfg.num_ops);
  std::mt19937_64 rng(cfg.seed);
  std::uniform_real_distribution<double> choose_op(0.0, 1.0);
  std::uniform_int_distribution<uint64_t> key_dist(0, cfg.key_space - 1);

  for (size_t i = 0; i < cfg.num_ops; ++i) {
    Op& op = (*workload)[i];
    op.type = pick_op_type(choose_op(rng), cfg);
    op.key = key_dist(rng);
    op.value = op.key ^ 0x9E3779B97F4A7C15ULL;
  }

  return workload;
}

std::unique_ptr<Workload> generate_zipfian(const WorkloadConfig& cfg, double alpha) {
  if (cfg.key_space == 0) {
    throw std::invalid_argument("key_space must be > 0");
  }
  auto workload = std::make_unique<Workload>(cfg.num_ops);
  std::mt19937_64 rng(cfg.seed);
  std::uniform_real_distribution<double> choose_op(0.0, 1.0);
  ZipfGenerator zipf(cfg.key_space, alpha, cfg.seed ^ 0xBADC0FFEEULL);

  for (size_t i = 0; i < cfg.num_ops; ++i) {
    Op& op = (*workload)[i];
    op.type = pick_op_type(choose_op(rng), cfg);
    op.key = zipf.sample();
    op.value = op.key ^ 0x9E3779B97F4A7C15ULL;
  }

  return workload;
}

std::unique_ptr<Workload> generate_shared_prefix(const WorkloadConfig& cfg) {
  if (cfg.prefix_count == 0) {
    throw std::invalid_argument("prefix_count must be > 0");
  }
  if (cfg.key_space == 0) {
    throw std::invalid_argument("key_space must be > 0 (serves as suffix space)");
  }
  auto workload = std::make_unique<Workload>(cfg.num_ops);
  std::mt19937_64 rng(cfg.seed);
  std::uniform_real_distribution<double> choose_op(0.0, 1.0);

  for (size_t i = 0; i < cfg.num_ops; ++i) {
    Op& op = (*workload)[i];
    op.type = pick_op_type(choose_op(rng), cfg);
    op.key = make_shared_prefix_key(rng, cfg.prefix_count, cfg.key_space);
    op.value = op.key ^ 0x9E3779B97F4A7C15ULL;
  }
  return workload;
}

std::unique_ptr<Workload> load_trace(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    throw std::runtime_error("failed to open trace: " + path);
  }

  uint32_t magic = 0;
  uint32_t version = 0;
  uint64_t num_ops = 0;
  in.read(reinterpret_cast<char*>(&magic), sizeof(magic));
  in.read(reinterpret_cast<char*>(&version), sizeof(version));
  in.read(reinterpret_cast<char*>(&num_ops), sizeof(num_ops));
  if (!in) {
    throw std::runtime_error("failed reading trace header: " + path);
  }
  if (magic != kTraceMagic) {
    throw std::runtime_error("trace header mismatch: " + path);
  }
  if (version != kTraceVersionV1 && version != kTraceVersionV2) {
    throw std::runtime_error("unsupported trace version: " + path);
  }
  if (version == kTraceVersionV2) {
    uint32_t provenance_len = 0;
    uint32_t reserved = 0;
    in.read(reinterpret_cast<char*>(&provenance_len), sizeof(provenance_len));
    in.read(reinterpret_cast<char*>(&reserved), sizeof(reserved));
    if (!in) {
      throw std::runtime_error("failed reading v2 provenance header: " + path);
    }
    in.seekg(provenance_len, std::ios::cur);
    if (!in) {
      throw std::runtime_error("failed skipping v2 provenance blob: " + path);
    }
  }

  auto workload = std::make_unique<Workload>(static_cast<size_t>(num_ops));
  for (uint64_t i = 0; i < num_ops; ++i) {
    uint8_t op_type = 0;
    uint8_t padding[7] = {};
    uint64_t key = 0;
    uint64_t value = 0;
    in.read(reinterpret_cast<char*>(&op_type), sizeof(op_type));
    in.read(reinterpret_cast<char*>(padding), sizeof(padding));
    in.read(reinterpret_cast<char*>(&key), sizeof(key));
    in.read(reinterpret_cast<char*>(&value), sizeof(value));
    if (!in) {
      throw std::runtime_error("trace ended early: " + path);
    }

    Op op;
    if (op_type == 0) {
      op.type = OpType::kInsert;
    } else if (op_type == 1) {
      op.type = OpType::kLookup;
    } else if (op_type == 2) {
      op.type = OpType::kRemove;
    } else {
      throw std::runtime_error("invalid op_type in trace");
    }
    op.key = key;
    op.value = value;
    (*workload)[static_cast<size_t>(i)] = op;
  }

  return workload;
}

std::vector<Key> prepopulate(HashTable& table, size_t n, uint64_t key_space,
                             uint64_t seed) {
  std::vector<Key> inserted;
  if (n == 0 || key_space == 0) {
    return inserted;
  }
  if (n > key_space) {
    n = static_cast<size_t>(key_space);
  }
  inserted.reserve(n);
  std::mt19937_64 rng(seed);
  std::uniform_int_distribution<uint64_t> dist(0, key_space - 1);
  std::unordered_set<Key> seen;
  seen.reserve(n * 2 + 1);

  const size_t max_attempts = n * 8;
  size_t attempts = 0;
  while (inserted.size() < n && attempts < max_attempts) {
    ++attempts;
    const Key key = dist(rng);
    if (!seen.insert(key).second) {
      continue;
    }
    const Value value = key ^ 0xD6E8FEB86659FD93ULL;
    if (table.insert(key, value)) {
      inserted.push_back(key);
    }
  }
  return inserted;
}

std::vector<Key> prepopulate_shared_prefix(HashTable& table, size_t n,
                                           uint32_t prefix_count,
                                           uint64_t suffix_space,
                                           uint64_t seed) {
  std::vector<Key> inserted;
  if (n == 0 || prefix_count == 0 || suffix_space == 0) {
    return inserted;
  }
  const uint64_t universe = static_cast<uint64_t>(prefix_count) * suffix_space;
  if (n > universe) {
    n = static_cast<size_t>(universe);
  }
  inserted.reserve(n);
  std::mt19937_64 rng(seed);
  std::unordered_set<Key> seen;
  seen.reserve(n * 2 + 1);

  const size_t max_attempts = n * 8;
  size_t attempts = 0;
  while (inserted.size() < n && attempts < max_attempts) {
    ++attempts;
    const Key key = make_shared_prefix_key(rng, prefix_count, suffix_space);
    if (!seen.insert(key).second) {
      continue;
    }
    const Value value = key ^ 0xD6E8FEB86659FD93ULL;
    if (table.insert(key, value)) {
      inserted.push_back(key);
    }
  }
  return inserted;
}

}  // namespace ee361c
