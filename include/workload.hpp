#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "hash_table.hpp"

namespace ee361c {

enum class OpType : uint8_t {
  kInsert = 0,
  kLookup = 1,
  kRemove = 2,
};

struct Op {
  OpType type;
  Key key;
  Value value;
};

class Workload {
public:
  explicit Workload(size_t count) : ops_(count) {}

  size_t size() const { return ops_.size(); }
  const Op& operator[](size_t i) const { return ops_[i]; }
  Op& operator[](size_t i) { return ops_[i]; }
  const Op* data() const { return ops_.data(); }
  Op* data() { return ops_.data(); }

private:
  std::vector<Op> ops_;
};

struct WorkloadConfig {
  size_t num_ops = 0;
  double read_fraction = 0.9;
  double insert_fraction = 0.5;
  uint64_t key_space = 10'000'000;
  uint64_t seed = 42;
  uint32_t prefix_count = 100;  // used by generate_shared_prefix only
};

std::unique_ptr<Workload> generate_uniform(const WorkloadConfig& cfg);
std::unique_ptr<Workload> generate_zipfian(const WorkloadConfig& cfg, double alpha);
std::unique_ptr<Workload> generate_shared_prefix(const WorkloadConfig& cfg);
std::unique_ptr<Workload> load_trace(const std::string& path);
// Pre-populate `table` with `n` distinct keys drawn from `[0, key_space)`.
// The sample space matches the uniform/zipfian workload key universe so that
// subsequent lookups can actually hit pre-inserted keys. Attempts are capped
// at `n * 8` to bound pathological cases where the table refuses further
// inserts (e.g. capacity ceiling, displacement budget); returns whatever was
// actually inserted.
std::vector<Key> prepopulate(HashTable& table, size_t n, uint64_t key_space,
                             uint64_t seed);

// Pre-populate `table` with `n` distinct shared-prefix keys drawn from the
// same distribution as generate_shared_prefix. Same capping semantics as
// prepopulate. prefix_count and suffix_space must both be > 0.
// Total key universe is prefix_count * suffix_space.
std::vector<Key> prepopulate_shared_prefix(HashTable& table, size_t n,
                                           uint32_t prefix_count,
                                           uint64_t suffix_space,
                                           uint64_t seed);

}  // namespace ee361c
