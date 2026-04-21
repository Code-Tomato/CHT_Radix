#include "hash_table.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <vector>

namespace ee361c {

namespace {

constexpr size_t kSlotsPerBucket = 4;
constexpr size_t kNumStripes = 64;
constexpr size_t kMaxDisplaceDepth = 64;
constexpr size_t kMaxBfsNodes = 20000;
constexpr Key kEmptyKey = std::numeric_limits<Key>::max();

inline uint64_t mix64(uint64_t x) {
  x ^= x >> 30;
  x *= 0xbf58476d1ce4e5b9ULL;
  x ^= x >> 27;
  x *= 0x94d049bb133111ebULL;
  x ^= x >> 31;
  return x;
}

inline uint64_t mix64_alt(uint64_t x) {
  x ^= x >> 33;
  x *= 0xff51afd7ed558ccdULL;
  x ^= x >> 33;
  x *= 0xc4ceb9fe1a85ec53ULL;
  x ^= x >> 33;
  return x;
}

inline size_t next_pow2(size_t x) {
  if (x <= 1) {
    return 1;
  }
  return size_t{1} << (64 - __builtin_clzll(x - 1));
}

struct Slot {
  Key key = kEmptyKey;
  Value value = kNoValue;
};

struct Bucket {
  std::array<Slot, kSlotsPerBucket> slots{};
};

struct Node {
  int table = 0;
  size_t bucket_idx = 0;
  int parent = -1;
  int parent_slot = -1;
  size_t depth = 0;
};

class CuckooStriped : public HashTable {
public:
  explicit CuckooStriped(size_t initial_capacity)
      : num_buckets_(next_pow2(std::max<size_t>(1, initial_capacity / 4))),
        bucket_mask_(num_buckets_ - 1),
        t1_(std::make_unique<Bucket[]>(num_buckets_)),
        t2_(std::make_unique<Bucket[]>(num_buckets_)) {}

  bool insert(Key key, Value value) override {
    if (key == kEmptyKey) {
      return false;
    }

    const size_t i1 = h1(key);
    const size_t i2 = h2(key);
    const size_t s1 = stripe_for(0, i1);
    const size_t s2 = stripe_for(1, i2);

    if (s1 == s2) {
      std::unique_lock<std::mutex> lk(stripes_[s1]);
      if (contains_key(0, i1, key) || contains_key(1, i2, key)) {
        return false;
      }
      if (place_if_empty(0, i1, key, value) || place_if_empty(1, i2, key, value)) {
        size_.fetch_add(1, std::memory_order_relaxed);
        return true;
      }
    } else {
      const size_t lo = std::min(s1, s2);
      const size_t hi = std::max(s1, s2);
      std::unique_lock<std::mutex> lk_lo(stripes_[lo]);
      std::unique_lock<std::mutex> lk_hi(stripes_[hi]);
      if (contains_key(0, i1, key) || contains_key(1, i2, key)) {
        return false;
      }
      if (place_if_empty(0, i1, key, value) || place_if_empty(1, i2, key, value)) {
        size_.fetch_add(1, std::memory_order_relaxed);
        return true;
      }
    }

    if (displace_insert(key, value, i1, i2)) {
      size_.fetch_add(1, std::memory_order_relaxed);
      return true;
    }
    return false;
  }

  bool lookup(Key key, Value& out) override {
    const size_t i1 = h1(key);
    const size_t i2 = h2(key);
    const size_t s1 = stripe_for(0, i1);
    const size_t s2 = stripe_for(1, i2);

    if (s1 == s2) {
      std::unique_lock<std::mutex> lk(stripes_[s1]);
      return lookup_under_lock(key, out, i1, i2);
    }
    const size_t lo = std::min(s1, s2);
    const size_t hi = std::max(s1, s2);
    std::unique_lock<std::mutex> lk_lo(stripes_[lo]);
    std::unique_lock<std::mutex> lk_hi(stripes_[hi]);
    return lookup_under_lock(key, out, i1, i2);
  }

  bool remove(Key key) override {
    const size_t i1 = h1(key);
    const size_t i2 = h2(key);
    const size_t s1 = stripe_for(0, i1);
    const size_t s2 = stripe_for(1, i2);

    if (s1 == s2) {
      std::unique_lock<std::mutex> lk(stripes_[s1]);
      if (remove_from_bucket(0, i1, key) || remove_from_bucket(1, i2, key)) {
        size_.fetch_sub(1, std::memory_order_relaxed);
        return true;
      }
      return false;
    }

    const size_t lo = std::min(s1, s2);
    const size_t hi = std::max(s1, s2);
    std::unique_lock<std::mutex> lk_lo(stripes_[lo]);
    std::unique_lock<std::mutex> lk_hi(stripes_[hi]);
    if (remove_from_bucket(0, i1, key) || remove_from_bucket(1, i2, key)) {
      size_.fetch_sub(1, std::memory_order_relaxed);
      return true;
    }
    return false;
  }

  size_t size() const override { return size_.load(std::memory_order_relaxed); }
  std::string name() const override { return "cuckoo_striped"; }

private:
  size_t h1(Key k) const { return mix64(k) & bucket_mask_; }
  size_t h2(Key k) const { return mix64_alt(k) & bucket_mask_; }

  size_t stripe_for(int table, size_t bucket_idx) const {
    return ((bucket_idx << 1) ^ static_cast<size_t>(table)) % kNumStripes;
  }

  Bucket& bucket(int table, size_t idx) {
    return table == 0 ? t1_[idx] : t2_[idx];
  }
  const Bucket& bucket(int table, size_t idx) const {
    return table == 0 ? t1_[idx] : t2_[idx];
  }

  size_t bucket_for_key(int table, Key key) const { return table == 0 ? h1(key) : h2(key); }

  bool contains_key(int table, size_t idx, Key key) const {
    const Bucket& b = bucket(table, idx);
    for (const auto& slot : b.slots) {
      if (slot.key == key) {
        return true;
      }
    }
    return false;
  }

  int find_empty_slot(int table, size_t idx) const {
    const Bucket& b = bucket(table, idx);
    for (size_t s = 0; s < kSlotsPerBucket; ++s) {
      if (b.slots[s].key == kEmptyKey) {
        return static_cast<int>(s);
      }
    }
    return -1;
  }

  bool place_if_empty(int table, size_t idx, Key key, Value value) {
    Bucket& b = bucket(table, idx);
    for (auto& slot : b.slots) {
      if (slot.key == kEmptyKey) {
        slot.key = key;
        slot.value = value;
        return true;
      }
    }
    return false;
  }

  bool remove_from_bucket(int table, size_t idx, Key key) {
    Bucket& b = bucket(table, idx);
    for (auto& slot : b.slots) {
      if (slot.key == key) {
        slot.key = kEmptyKey;
        slot.value = kNoValue;
        return true;
      }
    }
    return false;
  }

  bool lookup_under_lock(Key key, Value& out, size_t i1, size_t i2) const {
    const Bucket& b1 = bucket(0, i1);
    for (const auto& slot : b1.slots) {
      if (slot.key == key) {
        out = slot.value;
        return true;
      }
    }
    const Bucket& b2 = bucket(1, i2);
    for (const auto& slot : b2.slots) {
      if (slot.key == key) {
        out = slot.value;
        return true;
      }
    }
    out = kNoValue;
    return false;
  }

  bool displace_insert(Key key, Value value, size_t i1, size_t i2) {
    std::lock_guard<std::mutex> displace_guard(displace_mutex_);

    std::vector<std::unique_lock<std::mutex>> all_locks;
    all_locks.reserve(kNumStripes);
    for (size_t s = 0; s < kNumStripes; ++s) {
      all_locks.emplace_back(stripes_[s]);
    }

    if (contains_key(0, i1, key) || contains_key(1, i2, key)) {
      return false;
    }
    if (place_if_empty(0, i1, key, value) || place_if_empty(1, i2, key, value)) {
      return true;
    }

    std::vector<Node> nodes;
    nodes.reserve(256);
    std::queue<int> q;
    std::unordered_map<uint64_t, int> seen;
    seen.reserve(512);

    auto encode = [](int table, size_t idx) -> uint64_t {
      return (static_cast<uint64_t>(table) << 63) ^ static_cast<uint64_t>(idx);
    };

    auto push_node = [&](int table, size_t idx, int parent, int parent_slot, size_t depth) {
      const uint64_t code = encode(table, idx);
      if (seen.find(code) != seen.end()) {
        return;
      }
      const int id = static_cast<int>(nodes.size());
      nodes.push_back(Node{table, idx, parent, parent_slot, depth});
      seen.emplace(code, id);
      q.push(id);
    };

    push_node(0, i1, -1, -1, 0);
    push_node(1, i2, -1, -1, 0);

    int goal = -1;
    int goal_empty_slot = -1;

    while (!q.empty()) {
      const int cur_id = q.front();
      q.pop();
      const Node& cur = nodes[cur_id];
      if (cur.depth > kMaxDisplaceDepth) {
        continue;
      }

      goal_empty_slot = find_empty_slot(cur.table, cur.bucket_idx);
      if (goal_empty_slot >= 0 && cur.parent != -1) {
        goal = cur_id;
        break;
      }

      if (nodes.size() >= kMaxBfsNodes || cur.depth == kMaxDisplaceDepth) {
        continue;
      }

      const Bucket& b = bucket(cur.table, cur.bucket_idx);
      for (size_t s = 0; s < kSlotsPerBucket; ++s) {
        const Key k = b.slots[s].key;
        if (k == kEmptyKey) {
          continue;
        }
        const int next_table = 1 - cur.table;
        const size_t next_idx = bucket_for_key(next_table, k);
        push_node(next_table, next_idx, cur_id, static_cast<int>(s), cur.depth + 1);
      }
    }

    if (goal == -1) {
      return false;
    }

    int free_node_id = goal;
    int free_slot = goal_empty_slot;
    while (nodes[free_node_id].parent != -1) {
      const int parent_id = nodes[free_node_id].parent;
      const int parent_slot = nodes[free_node_id].parent_slot;

      Bucket& from_bucket = bucket(nodes[parent_id].table, nodes[parent_id].bucket_idx);
      Bucket& to_bucket = bucket(nodes[free_node_id].table, nodes[free_node_id].bucket_idx);
      to_bucket.slots[static_cast<size_t>(free_slot)] = from_bucket.slots[static_cast<size_t>(parent_slot)];
      from_bucket.slots[static_cast<size_t>(parent_slot)] = Slot{};

      free_slot = parent_slot;
      free_node_id = parent_id;
    }

    Bucket& root = bucket(nodes[free_node_id].table, nodes[free_node_id].bucket_idx);
    root.slots[static_cast<size_t>(free_slot)] = Slot{key, value};
    return true;
  }

  size_t num_buckets_;
  size_t bucket_mask_;
  std::unique_ptr<Bucket[]> t1_;
  std::unique_ptr<Bucket[]> t2_;
  mutable std::array<std::mutex, kNumStripes> stripes_{};
  std::mutex displace_mutex_;
  std::atomic<size_t> size_{0};
};

}  // namespace

std::unique_ptr<HashTable> make_cuckoo_striped(size_t initial_capacity) {
  return std::make_unique<CuckooStriped>(initial_capacity);
}

}  // namespace ee361c
