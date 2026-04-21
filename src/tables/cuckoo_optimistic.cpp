#include "hash_table.hpp"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <string>

#include "hash_utils.hpp"

namespace ee361c {

namespace {

constexpr size_t kSlotsPerBucket = 4;
constexpr size_t kMaxReadRetries = 100;
constexpr Key kEmptyKey = std::numeric_limits<Key>::max();

struct alignas(64) Bucket {
  std::atomic<uint32_t> version{0};  // Even=stable, odd=writer in progress.
  std::mutex mutex;
  std::atomic<Key> keys[kSlotsPerBucket];
  std::atomic<Value> values[kSlotsPerBucket];

  Bucket() {
    for (size_t i = 0; i < kSlotsPerBucket; ++i) {
      keys[i].store(kEmptyKey, std::memory_order_relaxed);
      values[i].store(kNoValue, std::memory_order_relaxed);
    }
  }
};

static_assert(alignof(Bucket) == 64, "Bucket should be cache-line aligned");

class CuckooOptimistic : public HashTable {
public:
  explicit CuckooOptimistic(size_t initial_capacity)
      : num_buckets_(next_pow2(std::max<size_t>(1, initial_capacity / 8))),
        bucket_mask_(num_buckets_ - 1),
        t1_(std::make_unique<Bucket[]>(num_buckets_)),
        t2_(std::make_unique<Bucket[]>(num_buckets_)) {}

  ~CuckooOptimistic() override = default;

  bool insert(Key key, Value value) override {
    assert(key != kEmptyKey);
    if (key == kEmptyKey) {
      return false;
    }

    const size_t i1 = h1(key);
    const size_t i2 = h2(key);
    Bucket* b1 = &t1_[i1];
    Bucket* b2 = &t2_[i2];

    lock_two_buckets(b1, b2);
    const bool already_present = contains_key(*b1, key) || contains_key(*b2, key);
    if (already_present) {
      unlock_two_buckets(b1, b2);
      return false;
    }

    if (try_place_in_bucket(*b1, key, value) || try_place_in_bucket(*b2, key, value)) {
      unlock_two_buckets(b1, b2);
      size_.fetch_add(1, std::memory_order_relaxed);
      return true;
    }
    unlock_two_buckets(b1, b2);

    if (cuckoo_displace_insert(key, value)) {
      size_.fetch_add(1, std::memory_order_relaxed);
      return true;
    }
    return false;
  }

  bool lookup(Key key, Value& out) override {
    const size_t i1 = h1(key);
    const size_t i2 = h2(key);
    Bucket& b1 = t1_[i1];
    Bucket& b2 = t2_[i2];

    for (size_t attempt = 0; attempt < kMaxReadRetries; ++attempt) {
      const uint32_t v1_start = b1.version.load(std::memory_order_acquire);
      const uint32_t v2_start = b2.version.load(std::memory_order_acquire);
      if ((v1_start & 1U) != 0U || (v2_start & 1U) != 0U) {
        continue;
      }

      Value candidate = kNoValue;
      bool found = false;
      int found_bucket = -1;
      for (size_t s = 0; s < kSlotsPerBucket; ++s) {
        const Key k = b1.keys[s].load(std::memory_order_relaxed);
        if (k == key) {
          candidate = b1.values[s].load(std::memory_order_relaxed);
          found = true;
          found_bucket = 1;
          break;
        }
      }
      if (!found) {
        for (size_t s = 0; s < kSlotsPerBucket; ++s) {
          const Key k = b2.keys[s].load(std::memory_order_relaxed);
          if (k == key) {
            candidate = b2.values[s].load(std::memory_order_relaxed);
            found = true;
            found_bucket = 2;
            break;
          }
        }
      }

      if (found) {
        if (found_bucket == 1) {
          const uint32_t v1_end = b1.version.load(std::memory_order_acquire);
          if (v1_end == v1_start) {
            out = candidate;
            return true;
          }
        } else {
          const uint32_t v2_end = b2.version.load(std::memory_order_acquire);
          if (v2_end == v2_start) {
            out = candidate;
            return true;
          }
        }
        continue;
      }

      const uint32_t v1_end = b1.version.load(std::memory_order_acquire);
      const uint32_t v2_end = b2.version.load(std::memory_order_acquire);
      if (v1_end == v1_start && v2_end == v2_start) {
        out = kNoValue;
        return false;
      }
    }

    return lookup_pessimistic(key, out);
  }

  bool remove(Key key) override {
    const size_t i1 = h1(key);
    const size_t i2 = h2(key);
    Bucket* b1 = &t1_[i1];
    Bucket* b2 = &t2_[i2];
    lock_two_buckets(b1, b2);

    for (Bucket* b : {b1, b2}) {
      for (size_t s = 0; s < kSlotsPerBucket; ++s) {
        if (b->keys[s].load(std::memory_order_relaxed) == key) {
          writer_begin(*b);
          b->values[s].store(kNoValue, std::memory_order_relaxed);
          b->keys[s].store(kEmptyKey, std::memory_order_release);
          writer_end(*b);
          unlock_two_buckets(b1, b2);
          size_.fetch_sub(1, std::memory_order_relaxed);
          return true;
        }
      }
    }

    unlock_two_buckets(b1, b2);
    return false;
  }

  size_t size() const override { return size_.load(std::memory_order_relaxed); }
  std::string name() const override { return "cuckoo_optimistic"; }

private:
  size_t h1(Key k) const { return mix64(k) & bucket_mask_; }
  size_t h2(Key k) const { return mix64_alt(k) & bucket_mask_; }

  Bucket& bucket(int table, size_t idx) { return table == 0 ? t1_[idx] : t2_[idx]; }

  bool contains_key(const Bucket& b, Key key) const {
    for (size_t s = 0; s < kSlotsPerBucket; ++s) {
      if (b.keys[s].load(std::memory_order_relaxed) == key) {
        return true;
      }
    }
    return false;
  }

  static void writer_begin(Bucket& b) { b.version.fetch_add(1, std::memory_order_acq_rel); }
  static void writer_end(Bucket& b) { b.version.fetch_add(1, std::memory_order_release); }

  bool try_place_in_bucket(Bucket& b, Key key, Value value) {
    for (size_t s = 0; s < kSlotsPerBucket; ++s) {
      if (b.keys[s].load(std::memory_order_relaxed) == kEmptyKey) {
        writer_begin(b);
        b.values[s].store(value, std::memory_order_relaxed);
        b.keys[s].store(key, std::memory_order_release);
        writer_end(b);
        return true;
      }
    }
    return false;
  }

  static void lock_two_buckets(Bucket* a, Bucket* b) {
    if (a == b) {
      a->mutex.lock();
      return;
    }
    if (a < b) {
      a->mutex.lock();
      b->mutex.lock();
    } else {
      b->mutex.lock();
      a->mutex.lock();
    }
  }

  static void unlock_two_buckets(Bucket* a, Bucket* b) {
    if (a == b) {
      a->mutex.unlock();
      return;
    }
    a->mutex.unlock();
    b->mutex.unlock();
  }

  bool lookup_pessimistic(Key key, Value& out) {
    const size_t i1 = h1(key);
    const size_t i2 = h2(key);
    Bucket* b1 = &t1_[i1];
    Bucket* b2 = &t2_[i2];
    lock_two_buckets(b1, b2);
    for (Bucket* b : {b1, b2}) {
      for (size_t s = 0; s < kSlotsPerBucket; ++s) {
        if (b->keys[s].load(std::memory_order_relaxed) == key) {
          out = b->values[s].load(std::memory_order_relaxed);
          unlock_two_buckets(b1, b2);
          return true;
        }
      }
    }
    out = kNoValue;
    unlock_two_buckets(b1, b2);
    return false;
  }

  bool cuckoo_displace_insert(Key key, Value value) {
    std::lock_guard<std::mutex> chain_guard(displace_mutex_);

    const size_t i1 = h1(key);
    const size_t i2 = h2(key);
    Bucket* b1 = &t1_[i1];
    Bucket* b2 = &t2_[i2];

    // Recheck under lock since other writers may have raced after fast-path unlock.
    lock_two_buckets(b1, b2);
    if (contains_key(*b1, key) || contains_key(*b2, key)) {
      unlock_two_buckets(b1, b2);
      return false;
    }
    if (try_place_in_bucket(*b1, key, value) || try_place_in_bucket(*b2, key, value)) {
      unlock_two_buckets(b1, b2);
      return true;
    }
    unlock_two_buckets(b1, b2);

    // Conservative one-hop displacement: only commit if victim has an alternate empty slot.
    for (int start_table = 0; start_table < 2; ++start_table) {
      Bucket* start_bucket = (start_table == 0) ? b1 : b2;
      for (size_t victim_slot = 0; victim_slot < kSlotsPerBucket; ++victim_slot) {
        const Key victim_key = start_bucket->keys[victim_slot].load(std::memory_order_relaxed);
        if (victim_key == kEmptyKey) {
          continue;
        }

        const int victim_other_table = 1 - start_table;
        const size_t victim_other_idx =
            (victim_other_table == 0) ? h1(victim_key) : h2(victim_key);
        Bucket* victim_other = &bucket(victim_other_table, victim_other_idx);
        lock_two_buckets(start_bucket, victim_other);

        int empty_slot = -1;
        for (size_t s = 0; s < kSlotsPerBucket; ++s) {
          if (victim_other->keys[s].load(std::memory_order_relaxed) == kEmptyKey) {
            empty_slot = static_cast<int>(s);
            break;
          }
        }
        if (empty_slot < 0) {
          unlock_two_buckets(start_bucket, victim_other);
          continue;
        }

        const Value victim_value =
            start_bucket->values[victim_slot].load(std::memory_order_relaxed);

        writer_begin(*victim_other);
        victim_other->values[static_cast<size_t>(empty_slot)].store(victim_value,
                                                                     std::memory_order_relaxed);
        victim_other->keys[static_cast<size_t>(empty_slot)].store(victim_key,
                                                                  std::memory_order_release);
        writer_end(*victim_other);

        writer_begin(*start_bucket);
        start_bucket->values[victim_slot].store(value, std::memory_order_relaxed);
        start_bucket->keys[victim_slot].store(key, std::memory_order_release);
        writer_end(*start_bucket);

        unlock_two_buckets(start_bucket, victim_other);
        return true;
      }
    }

    return false;
  }

  size_t num_buckets_;
  size_t bucket_mask_;
  std::unique_ptr<Bucket[]> t1_;
  std::unique_ptr<Bucket[]> t2_;
  std::mutex displace_mutex_;
  std::atomic<size_t> size_{0};
};

}  // namespace

std::unique_ptr<HashTable> make_cuckoo_optimistic(size_t initial_capacity) {
  return std::make_unique<CuckooOptimistic>(initial_capacity);
}

}  // namespace ee361c
