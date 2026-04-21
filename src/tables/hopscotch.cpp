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

constexpr size_t kHopRange = 32;
constexpr size_t kAddRange = 4096;   // Linear probe distance for a free slot.
constexpr size_t kMaxReadRetries = 100;
constexpr Key kEmptyKey = std::numeric_limits<Key>::max();

struct alignas(64) Bucket {
  std::atomic<Key> key{kEmptyKey};
  std::atomic<Value> value{kNoValue};
  // Bit d set means: bucket at (home + d) currently holds a key whose home
  // is *this* bucket. home is the bucket that owns hop_info.
  std::atomic<uint32_t> hop_info{0};
  std::atomic<uint32_t> version{0};
  std::mutex mutex;
};

static_assert(alignof(Bucket) == 64, "Bucket should be cache-line aligned");

class HopscotchOptimistic : public HashTable {
 public:
  explicit HopscotchOptimistic(size_t initial_capacity)
      : num_buckets_(next_pow2(std::max<size_t>(kHopRange * 2,
                                                initial_capacity * 2))),
        bucket_mask_(num_buckets_ - 1),
        buckets_(std::make_unique<Bucket[]>(num_buckets_)) {}

  ~HopscotchOptimistic() override = default;

  bool insert(Key key, Value value) override {
    assert(key != kEmptyKey);
    if (key == kEmptyKey) {
      return false;
    }

    // Serialize writers to keep displacement reasoning tractable. Readers
    // stay lock-free; this mirrors cuckoo_optimistic's displace guard.
    std::lock_guard<std::mutex> writer_guard(writer_mutex_);

    const size_t home = home_for(key);
    if (contains_key_under_writer(home, key)) {
      return false;
    }

    // Linear probe for the first free slot starting at home.
    size_t free_idx = num_buckets_;
    for (size_t d = 0; d < kAddRange; ++d) {
      const size_t idx = index(home + d);
      Bucket& b = buckets_[idx];
      if (b.key.load(std::memory_order_relaxed) == kEmptyKey) {
        free_idx = idx;
        break;
      }
    }
    if (free_idx == num_buckets_) {
      return false;
    }

    size_t cursor = free_idx;
    size_t distance = ring_distance(home, cursor);

    // Bubble the empty slot backward until it lands within kHopRange of home.
    while (distance >= kHopRange) {
      bool moved = false;
      // Try to pull a candidate from [cursor - (kHopRange - 1), cursor - 1].
      for (size_t gap = kHopRange - 1; gap > 0; --gap) {
        const size_t src_idx = index(cursor + num_buckets_ - gap);
        Bucket& src = buckets_[src_idx];
        const uint32_t hop = src.hop_info.load(std::memory_order_relaxed);
        if (hop == 0) {
          continue;
        }
        // Find a bit in hop whose absolute destination lies before cursor.
        for (size_t bit = 0; bit < gap; ++bit) {
          if ((hop & (1U << bit)) == 0) {
            continue;
          }
          const size_t victim_idx = index(src_idx + bit);
          Bucket& victim = buckets_[victim_idx];
          Bucket& dest = buckets_[cursor];

          acquire_two_buckets(&src, &dest, &victim);
          writer_begin(src);
          writer_begin(dest);
          if (&victim != &src && &victim != &dest) {
            writer_begin(victim);
          }

          const Key moved_key = victim.key.load(std::memory_order_relaxed);
          const Value moved_val = victim.value.load(std::memory_order_relaxed);

          dest.key.store(moved_key, std::memory_order_release);
          dest.value.store(moved_val, std::memory_order_relaxed);
          victim.key.store(kEmptyKey, std::memory_order_release);
          victim.value.store(kNoValue, std::memory_order_relaxed);

          // Flip hop_info bits for the source bucket.
          const size_t new_bit = ring_distance(src_idx, cursor);
          uint32_t new_hop = hop;
          new_hop &= ~(1U << bit);
          new_hop |= (1U << new_bit);
          src.hop_info.store(new_hop, std::memory_order_release);

          if (&victim != &src && &victim != &dest) {
            writer_end(victim);
          }
          writer_end(dest);
          writer_end(src);
          release_two_buckets(&src, &dest, &victim);

          cursor = victim_idx;
          distance = ring_distance(home, cursor);
          moved = true;
          break;
        }
        if (moved) {
          break;
        }
      }
      if (!moved) {
        return false;
      }
    }

    // cursor now names a free bucket within kHopRange of home; install the key.
    Bucket& home_bucket = buckets_[home];
    Bucket& target = buckets_[cursor];
    acquire_two_buckets(&home_bucket, &target, nullptr);
    writer_begin(home_bucket);
    if (&target != &home_bucket) {
      writer_begin(target);
    }

    target.value.store(value, std::memory_order_relaxed);
    target.key.store(key, std::memory_order_release);
    const uint32_t hop = home_bucket.hop_info.load(std::memory_order_relaxed);
    home_bucket.hop_info.store(hop | (1U << distance), std::memory_order_release);

    if (&target != &home_bucket) {
      writer_end(target);
    }
    writer_end(home_bucket);
    release_two_buckets(&home_bucket, &target, nullptr);

    size_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  // Reader protocol:
  //   Seqlock validation on the *home* bucket guards every operation that can
  //   observably change the bucket contents a reader sees.
  //
  // Invariant (proof sketch):
  //   Let K be a key currently stored in the table. Let H = home_for(K).
  //   Every mutation that can change whether a lookup of K observes the
  //   correct slot is one of:
  //     (a) insert of K into slot H+d within range  -> writer bumps H.version
  //         (final install locks home_bucket and bumps it, see `insert` below).
  //     (b) remove of K from some slot H+d          -> writer bumps H.version
  //         (`remove` locks home_bucket and bumps it).
  //     (c) bubble-up move of K from slot H+d to H+d' during someone else's
  //         insert -> the bubble step's `src` bucket in `insert` is always K's
  //         home H (src_idx owns the hop_info bit for K by construction),
  //         so the writer executes writer_begin(src) = writer_begin(H).
  //
  //   Therefore, any mutation that could change what a lookup of K observes
  //   bumps H.version around the mutation window. A reader that brackets its
  //   scan with two reads of H.version will either see an even version that
  //   matches end-to-end (consistent snapshot) or detect a change and retry.
  //
  //   Mutations to *foreign* keys that pass through H's neighborhood either
  //   (i) update H.hop_info (they involve a slot H+d owned by some other
  //   home B, but only because that key's bubble path lands in H's physical
  //   range; then B = home of that foreign key, B.version is bumped, and H's
  //   hop_info is untouched since no bit belongs to K), or (ii) don't touch
  //   H at all. In case (i), a reader of K scanning H.hop_info never sees a
  //   bit for the foreign key since H.hop_info only tracks keys whose home
  //   is H. In case (ii), the foreign mutation is irrelevant to the reader.
  //
  // Retry bound: kMaxReadRetries; after that fall back to the pessimistic
  // path under home_bucket.mutex. This keeps lookups wait-free in the common
  // case and linearizable under adversarial scheduling.
  bool lookup(Key key, Value& out) override {
    const size_t home = home_for(key);
    Bucket& home_bucket = buckets_[home];

    for (size_t attempt = 0; attempt < kMaxReadRetries; ++attempt) {
      const uint32_t v_start = home_bucket.version.load(std::memory_order_acquire);
      if ((v_start & 1U) != 0U) {
        continue;
      }
      const uint32_t hop = home_bucket.hop_info.load(std::memory_order_acquire);
      uint32_t remaining = hop;

      Value candidate = kNoValue;
      bool found = false;
      while (remaining != 0U) {
        const size_t bit = static_cast<size_t>(__builtin_ctz(remaining));
        remaining &= remaining - 1U;
        Bucket& b = buckets_[index(home + bit)];
        if (b.key.load(std::memory_order_acquire) == key) {
          candidate = b.value.load(std::memory_order_acquire);
          found = true;
          break;
        }
      }

      const uint32_t v_end = home_bucket.version.load(std::memory_order_acquire);
      if (v_end != v_start) {
        continue;
      }
      if (found) {
        out = candidate;
        return true;
      }
      out = kNoValue;
      return false;
    }

    return lookup_pessimistic(key, out);
  }

  bool remove(Key key) override {
    std::lock_guard<std::mutex> writer_guard(writer_mutex_);

    const size_t home = home_for(key);
    Bucket& home_bucket = buckets_[home];
    uint32_t hop = home_bucket.hop_info.load(std::memory_order_relaxed);
    uint32_t remaining = hop;
    while (remaining != 0U) {
      const size_t bit = static_cast<size_t>(__builtin_ctz(remaining));
      remaining &= remaining - 1U;
      Bucket& b = buckets_[index(home + bit)];
      if (b.key.load(std::memory_order_relaxed) == key) {
        acquire_two_buckets(&home_bucket, &b, nullptr);
        writer_begin(home_bucket);
        if (&b != &home_bucket) {
          writer_begin(b);
        }

        b.key.store(kEmptyKey, std::memory_order_release);
        b.value.store(kNoValue, std::memory_order_relaxed);
        home_bucket.hop_info.store(hop & ~(1U << bit), std::memory_order_release);

        if (&b != &home_bucket) {
          writer_end(b);
        }
        writer_end(home_bucket);
        release_two_buckets(&home_bucket, &b, nullptr);

        size_.fetch_sub(1, std::memory_order_relaxed);
        return true;
      }
    }
    return false;
  }

  size_t size() const override { return size_.load(std::memory_order_relaxed); }
  std::string name() const override { return "hopscotch"; }

 private:
  size_t home_for(Key k) const { return mix64(k) & bucket_mask_; }
  size_t index(size_t raw) const { return raw & bucket_mask_; }

  size_t ring_distance(size_t from, size_t to) const {
    return (to + num_buckets_ - from) & bucket_mask_;
  }

  static void writer_begin(Bucket& b) {
    b.version.fetch_add(1, std::memory_order_acq_rel);
  }
  static void writer_end(Bucket& b) {
    b.version.fetch_add(1, std::memory_order_release);
  }

  // Lock up to three buckets in address order without double-locking.
  static void acquire_two_buckets(Bucket* a, Bucket* b, Bucket* c) {
    Bucket* items[3] = {a, b, c};
    // Sort pointers (small n, insertion sort).
    for (size_t i = 1; i < 3; ++i) {
      for (size_t j = i; j > 0 && pointer_less(items[j], items[j - 1]); --j) {
        Bucket* tmp = items[j];
        items[j] = items[j - 1];
        items[j - 1] = tmp;
      }
    }
    Bucket* prev = nullptr;
    for (Bucket* item : items) {
      if (item == nullptr || item == prev) {
        continue;
      }
      item->mutex.lock();
      prev = item;
    }
  }

  static void release_two_buckets(Bucket* a, Bucket* b, Bucket* c) {
    Bucket* items[3] = {a, b, c};
    for (size_t i = 1; i < 3; ++i) {
      for (size_t j = i; j > 0 && pointer_less(items[j], items[j - 1]); --j) {
        Bucket* tmp = items[j];
        items[j] = items[j - 1];
        items[j - 1] = tmp;
      }
    }
    Bucket* prev = nullptr;
    for (Bucket* item : items) {
      if (item == nullptr || item == prev) {
        continue;
      }
      item->mutex.unlock();
      prev = item;
    }
  }

  static bool pointer_less(const Bucket* a, const Bucket* b) {
    if (a == nullptr) {
      return false;
    }
    if (b == nullptr) {
      return true;
    }
    return a < b;
  }

  bool contains_key_under_writer(size_t home, Key key) const {
    Bucket& home_bucket = buckets_[home];
    uint32_t hop = home_bucket.hop_info.load(std::memory_order_relaxed);
    while (hop != 0U) {
      const size_t bit = static_cast<size_t>(__builtin_ctz(hop));
      hop &= hop - 1U;
      const Bucket& b = buckets_[index(home + bit)];
      if (b.key.load(std::memory_order_relaxed) == key) {
        return true;
      }
    }
    return false;
  }

  bool lookup_pessimistic(Key key, Value& out) {
    Bucket& home_bucket = buckets_[home_for(key)];
    std::lock_guard<std::mutex> guard(home_bucket.mutex);
    uint32_t hop = home_bucket.hop_info.load(std::memory_order_acquire);
    while (hop != 0U) {
      const size_t bit = static_cast<size_t>(__builtin_ctz(hop));
      hop &= hop - 1U;
      Bucket& b = buckets_[index(home_for(key) + bit)];
      if (b.key.load(std::memory_order_relaxed) == key) {
        out = b.value.load(std::memory_order_relaxed);
        return true;
      }
    }
    out = kNoValue;
    return false;
  }

  size_t num_buckets_;
  size_t bucket_mask_;
  std::unique_ptr<Bucket[]> buckets_;
  std::mutex writer_mutex_;
  std::atomic<size_t> size_{0};
};

}  // namespace

std::unique_ptr<HashTable> make_hopscotch(size_t initial_capacity) {
  return std::make_unique<HopscotchOptimistic>(initial_capacity);
}

}  // namespace ee361c
