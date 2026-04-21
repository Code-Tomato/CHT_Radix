#include <atomic>
#include <cstdint>
#include <exception>
#include <iostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "hash_table.hpp"

namespace ee361c {
namespace {

struct TestConfig {
  size_t threads = 8;
  size_t repeats = 1;
  std::vector<std::string> tables = {"stub", "chaining_coarse", "chaining_fine",
                                     "cuckoo_optimistic", "cuckoo_striped",
                                     "hopscotch", "radix_tree_coarse",
                                     "radix_tree_fine"};
};

std::vector<std::string> split_csv(const std::string& s) {
  std::vector<std::string> out;
  std::stringstream ss(s);
  std::string part;
  while (std::getline(ss, part, ',')) {
    if (!part.empty()) {
      out.push_back(part);
    }
  }
  return out;
}

TestConfig parse_args(int argc, char** argv) {
  TestConfig cfg;
  for (int i = 1; i < argc; ++i) {
    const std::string arg(argv[i]);
    if (arg.rfind("--threads=", 0) == 0) {
      std::stringstream ss(arg.substr(std::string("--threads=").size()));
      ss >> cfg.threads;
      if (!ss || !ss.eof() || cfg.threads == 0) {
        throw std::invalid_argument("invalid --threads value");
      }
      continue;
    }
    if (arg.rfind("--repeats=", 0) == 0) {
      std::stringstream ss(arg.substr(std::string("--repeats=").size()));
      ss >> cfg.repeats;
      if (!ss || !ss.eof() || cfg.repeats == 0) {
        throw std::invalid_argument("invalid --repeats value");
      }
      continue;
    }
    if (arg.rfind("--tables=", 0) == 0) {
      cfg.tables = split_csv(arg.substr(std::string("--tables=").size()));
      if (cfg.tables.empty()) {
        throw std::invalid_argument("invalid --tables value");
      }
      continue;
    }
    throw std::invalid_argument("unknown flag: " + arg);
  }
  return cfg;
}

bool single_thread_sanity(HashTable& table) {
  constexpr uint64_t kN = 100'000;
  for (uint64_t i = 0; i < kN; ++i) {
    if (!table.insert(i, i ^ 0x1234ULL)) {
      std::cerr << "single_thread_sanity: insert failed at " << i << "\n";
      return false;
    }
  }
  for (uint64_t i = 0; i < kN; ++i) {
    Value out = kNoValue;
    if (!table.lookup(i, out) || out != (i ^ 0x1234ULL)) {
      std::cerr << "single_thread_sanity: lookup mismatch at " << i << "\n";
      return false;
    }
  }
  for (uint64_t i = 0; i < kN; ++i) {
    if (!table.remove(i)) {
      std::cerr << "single_thread_sanity: remove failed at " << i << "\n";
      return false;
    }
  }
  if (table.size() != 0) {
    std::cerr << "single_thread_sanity: size not zero after removals\n";
    return false;
  }
  return true;
}

bool concurrent_insert(HashTable& table, size_t threads_count) {
  constexpr uint64_t kPerThread = 100'000;
  std::vector<std::thread> threads;
  threads.reserve(threads_count);

  for (size_t t = 0; t < threads_count; ++t) {
    threads.emplace_back([&table, t]() {
      const uint64_t base = static_cast<uint64_t>(t) * 1'000'000ULL;
      for (uint64_t i = 0; i < kPerThread; ++i) {
        table.insert(base + i, (base + i) ^ 0x7777ULL);
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }

  const uint64_t expected = kPerThread * threads_count;
  for (size_t t = 0; t < threads_count; ++t) {
    const uint64_t base = static_cast<uint64_t>(t) * 1'000'000ULL;
    for (uint64_t i = 0; i < kPerThread; ++i) {
      Value out = kNoValue;
      if (!table.lookup(base + i, out) || out != ((base + i) ^ 0x7777ULL)) {
        std::cerr << "concurrent_insert: key verification failed (thread=" << t
                  << ", key=" << (base + i) << ")\n";
        return false;
      }
    }
  }
  if (table.size() != expected) {
    std::cerr << "concurrent_insert: expected size " << expected << ", got " << table.size()
              << "\n";
    return false;
  }
  return true;
}

bool concurrent_mixed(HashTable& table, size_t threads_count) {
  constexpr uint64_t kStable = 250'000;
  constexpr uint64_t kMutable = 250'000;
  constexpr uint64_t kOpsPerThread = 100'000;

  for (uint64_t i = 0; i < kStable; ++i) {
    if (!table.insert(i, i + 1)) {
      std::cerr << "concurrent_mixed: failed to preinsert stable key " << i << "\n";
      return false;
    }
  }
  for (uint64_t i = 0; i < kMutable; ++i) {
    if (!table.insert(10'000'000 + i, (10'000'000 + i) + 1)) {
      std::cerr << "concurrent_mixed: failed to preinsert mutable key " << i << "\n";
      return false;
    }
  }

  std::atomic<uint64_t> lookup_errors{0};
  std::vector<std::thread> threads;
  threads.reserve(threads_count);

  for (size_t t = 0; t < threads_count; ++t) {
    threads.emplace_back([&table, &lookup_errors, t]() {
      std::mt19937_64 rng(static_cast<uint64_t>(t) * 0x9E3779B97F4A7C15ULL + 42);
      std::uniform_int_distribution<int> op_dist(0, 99);
      std::uniform_int_distribution<uint64_t> stable_key(0, kStable - 1);
      std::uniform_int_distribution<uint64_t> mutable_key(10'000'000, 10'000'000 + kMutable - 1);
      std::uniform_int_distribution<uint64_t> insert_key(20'000'000 + t * kOpsPerThread,
                                                         20'000'000 + (t + 1) * kOpsPerThread);

      for (uint64_t i = 0; i < kOpsPerThread; ++i) {
        const int which = op_dist(rng);
        if (which < 50) {
          const uint64_t k = stable_key(rng);
          Value out = kNoValue;
          if (!table.lookup(k, out) || out != (k + 1)) {
            lookup_errors.fetch_add(1, std::memory_order_relaxed);
          }
        } else if (which < 75) {
          const uint64_t k = insert_key(rng);
          table.insert(k, k + 1);
        } else {
          table.remove(mutable_key(rng));
        }
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }

  if (lookup_errors.load(std::memory_order_relaxed) != 0) {
    std::cerr << "concurrent_mixed: lookup errors observed: "
              << lookup_errors.load(std::memory_order_relaxed) << "\n";
    return false;
  }
  return true;
}

bool displacement_stress(HashTable& table, size_t threads_count) {
  // Targets a high (but not maximum) load factor and only verifies keys that
  // were actually inserted. Fixed-size tables may refuse inserts before they
  // hit 100% capacity; that's a documented limitation, not a correctness bug.
  // Any *lookup* of a successfully-inserted and never-removed key that misses
  // is a real concurrency bug.
  constexpr uint64_t kPrepopulateTarget = 1'200'000;
  constexpr uint64_t kStable = 250'000;
  constexpr uint64_t kOpsPerThread = 100'000;
  constexpr uint64_t kInsertBase = 40'000'000;

  uint64_t inserted_count = 0;
  for (uint64_t i = 0; i < kPrepopulateTarget; ++i) {
    if (table.insert(1'000'000 + i, (1'000'000 + i) ^ 0xBEEFULL)) {
      ++inserted_count;
    }
  }
  if (inserted_count < kStable) {
    std::cerr << "displacement_stress: only " << inserted_count
              << " prepopulate inserts accepted; need at least " << kStable << "\n";
    return false;
  }

  std::atomic<uint64_t> lookup_errors{0};
  std::vector<std::thread> threads;
  threads.reserve(threads_count);
  for (size_t t = 0; t < threads_count; ++t) {
    threads.emplace_back([&table, &lookup_errors, t]() {
      std::mt19937_64 rng(static_cast<uint64_t>(t) * 0x9E3779B97F4A7C15ULL + 123);
      std::uniform_int_distribution<int> op_dist(0, 99);
      std::uniform_int_distribution<uint64_t> stable_dist(1'000'000, 1'000'000 + kStable - 1);
      std::uniform_int_distribution<uint64_t> remove_dist(1'000'000 + kStable,
                                                          1'000'000 + kPrepopulateTarget - 1);

      for (uint64_t i = 0; i < kOpsPerThread; ++i) {
        const int op = op_dist(rng);
        if (op < 70) {
          const uint64_t k = stable_dist(rng);
          Value out = kNoValue;
          if (!table.lookup(k, out) || out != (k ^ 0xBEEFULL)) {
            lookup_errors.fetch_add(1, std::memory_order_relaxed);
          }
        } else if (op < 90) {
          const uint64_t k = kInsertBase + static_cast<uint64_t>(t) * kOpsPerThread + i;
          table.insert(k, k ^ 0xCAFEULL);  // may return false near capacity; expected
        } else {
          table.remove(remove_dist(rng));
        }
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }
  const uint64_t lerr = lookup_errors.load(std::memory_order_relaxed);
  if (lerr != 0) {
    std::cerr << "displacement_stress: lookup errors observed: " << lerr << "\n";
    return false;
  }
  return true;
}

bool multi_writer_displacement(HashTable& table, size_t threads_count,
                               const std::string& table_name) {
  // Many writers hammer a shared dense hot range of keys into a small table,
  // forcing repeated failed attempts and displacement chains. Readers look up
  // a pre-populated stable set; any reader miss on a never-removed stable key
  // is a correctness bug.
  constexpr uint64_t kStableBase = 2'000'000;
  constexpr uint64_t kStableCount = 40'000;
  constexpr uint64_t kHotBase = 50'000'000;
  constexpr uint64_t kHotKeys = 120'000;
  constexpr uint64_t kWriterIters = 60'000;
  constexpr uint64_t kReaderScans = 200'000;

  uint64_t stable_inserted = 0;
  for (uint64_t i = 0; i < kStableCount; ++i) {
    if (table.insert(kStableBase + i, (kStableBase + i) ^ 0xDEADULL)) {
      ++stable_inserted;
    }
  }
  if (stable_inserted < 1000) {
    std::cerr << "multi_writer_displacement: only " << stable_inserted
              << " stable inserts landed\n";
    return false;
  }

  const size_t reader_count = std::min<size_t>(2, threads_count);
  const size_t writer_count = threads_count > reader_count
                                  ? threads_count - reader_count
                                  : 1;

  std::atomic<uint64_t> reader_errors{0};
  std::atomic<uint64_t> insert_attempts{0};
  std::atomic<uint64_t> insert_successes{0};
  std::atomic<bool> writers_done{false};

  std::vector<std::thread> threads;
  threads.reserve(threads_count);

  for (size_t t = 0; t < writer_count; ++t) {
    threads.emplace_back([&table, &insert_attempts, &insert_successes, t]() {
      // Shared dense prefix: every writer hashes into the same hot range so
      // they collide on buckets and exercise the displacement path.
      std::mt19937_64 rng(static_cast<uint64_t>(t) * 0x9E3779B97F4A7C15ULL + 31);
      std::uniform_int_distribution<uint64_t> dist(kHotBase, kHotBase + kHotKeys - 1);
      for (uint64_t i = 0; i < kWriterIters; ++i) {
        const uint64_t key = dist(rng);
        insert_attempts.fetch_add(1, std::memory_order_relaxed);
        if (table.insert(key, key ^ 0xFACEULL)) {
          insert_successes.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  for (size_t r = 0; r < reader_count; ++r) {
    threads.emplace_back(
        [&table, &reader_errors, &writers_done, stable_inserted, r]() {
          std::mt19937_64 rng(static_cast<uint64_t>(r) * 0x7F4A7C15ULL + 7);
          std::uniform_int_distribution<uint64_t> dist(
              kStableBase, kStableBase + stable_inserted - 1);
          uint64_t scans = 0;
          while (!writers_done.load(std::memory_order_acquire) ||
                 scans < kReaderScans) {
            const uint64_t key = dist(rng);
            Value out = kNoValue;
            if (!table.lookup(key, out) || out != (key ^ 0xDEADULL)) {
              reader_errors.fetch_add(1, std::memory_order_relaxed);
            }
            ++scans;
          }
        });
  }

  for (size_t t = 0; t < writer_count; ++t) {
    threads[t].join();
  }
  writers_done.store(true, std::memory_order_release);
  for (size_t r = 0; r < reader_count; ++r) {
    threads[writer_count + r].join();
  }

  const uint64_t rerr = reader_errors.load(std::memory_order_relaxed);
  if (rerr != 0) {
    std::cerr << "multi_writer_displacement: reader errors: " << rerr << "\n";
    return false;
  }

  // Confirm we actually exercised displacement: with ~180K writer ops landing
  // into a bucket set already holding duplicates from prior iterations and
  // colliding with 40K stable keys, at least some inserts must return false.
  // Chaining variants dedup by equality so false is expected; cuckoo/hopscotch
  // additionally fail when displacement budget is exhausted. Either way, a
  // fully-100% success rate means the hot range is too large for this table
  // -> the displacement path wasn't exercised and this test didn't do its
  // job.
  const uint64_t attempts = insert_attempts.load(std::memory_order_relaxed);
  const uint64_t successes = insert_successes.load(std::memory_order_relaxed);
  if (attempts == 0 || successes == attempts) {
    std::cerr << "multi_writer_displacement[" << table_name
              << "]: did not exercise displacement; "
              << successes << "/" << attempts << " inserts succeeded\n";
    return false;
  }
  return true;
}

bool inserter_lookup_race(HashTable& table) {
  constexpr size_t kN = 50'000;
  constexpr uint64_t kBase = 90'000'000;
  std::vector<std::atomic<uint8_t>> published(kN);
  for (size_t i = 0; i < kN; ++i) {
    published[i].store(0, std::memory_order_relaxed);
  }
  std::atomic<bool> done{false};
  std::atomic<uint64_t> errors{0};

  std::thread inserter([&]() {
    for (size_t i = 0; i < kN; ++i) {
      const uint64_t key = kBase + static_cast<uint64_t>(i);
      table.insert(key, key + 1);
      published[i].store(1, std::memory_order_release);
    }
    done.store(true, std::memory_order_release);
  });

  std::thread lookup([&]() {
    while (!done.load(std::memory_order_acquire)) {
      for (size_t i = 0; i < kN; ++i) {
        if (published[i].load(std::memory_order_acquire) == 0) {
          continue;
        }
        const uint64_t key = kBase + static_cast<uint64_t>(i);
        Value out = kNoValue;
        if (!table.lookup(key, out) || out != key + 1) {
          errors.fetch_add(1, std::memory_order_relaxed);
        }
      }
    }
  });

  inserter.join();
  lookup.join();
  if (errors.load(std::memory_order_relaxed) != 0) {
    std::cerr << "inserter_lookup_race: lookup errors observed: "
              << errors.load(std::memory_order_relaxed) << "\n";
    return false;
  }
  return true;
}

bool hopscotch_bubble_race(HashTable& table, size_t threads_count) {
  // Adversarial probe specifically aimed at the hopscotch bubble-up path.
  // Callers MUST size this table small enough that bubbles actually fire;
  // run_for_table uses a 200K capacity (rounds to 512K buckets) so the
  // 200K stable + 400K cascade targets ~117% of buckets and forces both
  // long bubble chains and the displacement-budget failure path. We
  // prepopulate a stable key set (known-present, never-removed) then one
  // thread cascades *other* inserts into the same neighborhood to force
  // long bubble chains, while every other thread continuously looks up
  // keys from the stable set. Any reader miss on a stable key is a bug
  // in either the neighborhood bubble logic or the reader's version check.
  //
  // The invariant argument behind the seqlock read is about the *victim*
  // key in each bubble step: its home bucket is exactly `src`, and every
  // bubble step writer_begin(src), so readers revalidating home.version
  // catch any movement. The inserted key's home bucket is unrelated to
  // intermediate bubble steps; this test does not directly probe it.
  //
  // We also track insert return values: if every cascade insert succeeds,
  // the test didn't stress the high-load bubble or displacement-budget
  // paths, so the validation is weak. The test fails in that case so we
  // don't claim coverage we didn't exercise.
  constexpr uint64_t kStableCount = 200'000;
  constexpr uint64_t kStableBase = 5'000'000;
  constexpr uint64_t kCascadeKeys = 400'000;
  constexpr uint64_t kCascadeBase = 9'000'000;
  constexpr uint64_t kReaderOps = 400'000;

  uint64_t stable_inserted = 0;
  for (uint64_t i = 0; i < kStableCount; ++i) {
    if (table.insert(kStableBase + i, (kStableBase + i) ^ 0x51AB1EULL)) {
      ++stable_inserted;
    }
  }
  if (stable_inserted < 10'000) {
    std::cerr << "hopscotch_bubble_race: only " << stable_inserted
              << " stable inserts landed\n";
    return false;
  }

  std::atomic<bool> writer_done{false};
  std::atomic<uint64_t> reader_errors{0};
  std::atomic<uint64_t> cascade_attempts{0};
  std::atomic<uint64_t> cascade_successes{0};

  std::thread cascader([&table, &writer_done, &cascade_attempts,
                        &cascade_successes]() {
    for (uint64_t i = 0; i < kCascadeKeys; ++i) {
      cascade_attempts.fetch_add(1, std::memory_order_relaxed);
      if (table.insert(kCascadeBase + i, (kCascadeBase + i) ^ 0xCA5CULL)) {
        cascade_successes.fetch_add(1, std::memory_order_relaxed);
      }
    }
    writer_done.store(true, std::memory_order_release);
  });

  const size_t reader_count = threads_count > 1 ? threads_count - 1 : 1;
  std::vector<std::thread> readers;
  readers.reserve(reader_count);
  for (size_t r = 0; r < reader_count; ++r) {
    readers.emplace_back(
        [&table, &reader_errors, &writer_done, stable_inserted, r]() {
          std::mt19937_64 rng(static_cast<uint64_t>(r) * 0x9E37ULL + 0xB1B1);
          std::uniform_int_distribution<uint64_t> dist(
              kStableBase, kStableBase + stable_inserted - 1);
          uint64_t ops = 0;
          while (!writer_done.load(std::memory_order_acquire) ||
                 ops < kReaderOps) {
            const uint64_t key = dist(rng);
            Value out = kNoValue;
            if (!table.lookup(key, out) || out != (key ^ 0x51AB1EULL)) {
              reader_errors.fetch_add(1, std::memory_order_relaxed);
            }
            ++ops;
          }
        });
  }

  cascader.join();
  for (auto& r : readers) {
    r.join();
  }
  const uint64_t rerr = reader_errors.load(std::memory_order_relaxed);
  if (rerr != 0) {
    std::cerr << "hopscotch_bubble_race: reader errors: " << rerr << "\n";
    return false;
  }
  const uint64_t attempts = cascade_attempts.load(std::memory_order_relaxed);
  const uint64_t successes = cascade_successes.load(std::memory_order_relaxed);
  if (attempts == 0 || successes == attempts) {
    std::cerr << "hopscotch_bubble_race: did not exercise bubble pressure; "
              << successes << "/" << attempts << " cascade inserts succeeded "
              << "(need insert failures to confirm displacement budget hit)\n";
    return false;
  }
  return true;
}

bool read_heavy_mixed(HashTable& table, size_t threads_count) {
  // Read-heavy concurrent mix on a shared key set. Designed to catch read-
  // path visibility bugs (torn reads, stale acquire-load ordering) that
  // the write-dominant `single_key_churn` can miss.
  //
  // Invariant: any lookup that returns true must observe exactly the
  // deterministic value for its key. Misses are fine (a concurrent remove
  // may have just fired) — we don't count those.
  constexpr uint64_t kBase = 80'000'000;
  constexpr uint64_t kKeys = 10'000;
  constexpr uint64_t kOpsPerThread = 50'000;

  auto value_for = [](uint64_t key) -> Value {
    return key ^ 0xFEEDFACECAFEBEEFULL;
  };

  for (uint64_t i = 0; i < kKeys; ++i) {
    const uint64_t k = kBase + i;
    if (!table.insert(k, value_for(k))) {
      std::cerr << "read_heavy_mixed: prepopulate insert failed at " << i << "\n";
      return false;
    }
  }

  std::atomic<uint64_t> torn_reads{0};

  std::vector<std::thread> threads;
  threads.reserve(threads_count);
  for (size_t t = 0; t < threads_count; ++t) {
    threads.emplace_back([&table, &torn_reads, &value_for, t]() {
      std::mt19937_64 rng(static_cast<uint64_t>(t) * 0xB01DFACEULL + 29);
      std::uniform_int_distribution<uint64_t> key_dist(kBase,
                                                        kBase + kKeys - 1);
      std::uniform_real_distribution<double> op_dist(0.0, 1.0);
      for (uint64_t i = 0; i < kOpsPerThread; ++i) {
        const uint64_t k = key_dist(rng);
        const double r = op_dist(rng);
        if (r < 0.90) {
          Value out = kNoValue;
          if (table.lookup(k, out)) {
            if (out != value_for(k)) {
              torn_reads.fetch_add(1, std::memory_order_relaxed);
            }
          }
        } else if (r < 0.95) {
          table.remove(k);
        } else {
          table.insert(k, value_for(k));
        }
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }

  const uint64_t errs = torn_reads.load(std::memory_order_relaxed);
  if (errs != 0) {
    std::cerr << "read_heavy_mixed: " << errs
              << " reads returned a value inconsistent with the key "
              << "(torn or stale)\n";
    return false;
  }
  return true;
}

bool single_key_churn(HashTable& table, size_t threads_count) {
  // Writers remove + re-insert keys from a shared churn set.
  // Readers look up from the same set and must observe either:
  //   (a) a miss (the key was momentarily absent), or
  //   (b) a hit with the exactly-correct deterministic value for that key.
  // Any hit with a wrong value is a torn read / staleness bug.
  constexpr uint64_t kChurnBase = 70'000'000;
  constexpr uint64_t kChurnKeys = 10'000;
  constexpr uint64_t kWriterOps = 100'000;
  constexpr uint64_t kReaderOps = 200'000;

  auto value_for = [](uint64_t key) -> Value {
    return key ^ 0x51AFCBABEULL;
  };

  for (uint64_t i = 0; i < kChurnKeys; ++i) {
    const uint64_t k = kChurnBase + i;
    if (!table.insert(k, value_for(k))) {
      std::cerr << "single_key_churn: prepopulate insert failed at " << i << "\n";
      return false;
    }
  }

  const size_t writer_count = std::max<size_t>(1, threads_count / 2);
  const size_t reader_count = std::max<size_t>(1, threads_count - writer_count);

  std::atomic<uint64_t> torn_reads{0};
  std::atomic<bool> writers_done{false};

  std::vector<std::thread> threads;
  threads.reserve(writer_count + reader_count);

  for (size_t w = 0; w < writer_count; ++w) {
    threads.emplace_back([&table, &value_for, w]() {
      std::mt19937_64 rng(static_cast<uint64_t>(w) * 0xC0FFEEULL + 11);
      std::uniform_int_distribution<uint64_t> dist(kChurnBase,
                                                    kChurnBase + kChurnKeys - 1);
      for (uint64_t i = 0; i < kWriterOps; ++i) {
        const uint64_t k = dist(rng);
        table.remove(k);
        table.insert(k, value_for(k));
      }
    });
  }

  for (size_t r = 0; r < reader_count; ++r) {
    threads.emplace_back([&table, &torn_reads, &writers_done, &value_for, r]() {
      std::mt19937_64 rng(static_cast<uint64_t>(r) * 0xD0D0DULL + 17);
      std::uniform_int_distribution<uint64_t> dist(kChurnBase,
                                                    kChurnBase + kChurnKeys - 1);
      uint64_t ops = 0;
      while (!writers_done.load(std::memory_order_acquire) || ops < kReaderOps) {
        const uint64_t k = dist(rng);
        Value out = kNoValue;
        if (table.lookup(k, out)) {
          if (out != value_for(k)) {
            torn_reads.fetch_add(1, std::memory_order_relaxed);
          }
        }
        ++ops;
      }
    });
  }

  for (size_t w = 0; w < writer_count; ++w) {
    threads[w].join();
  }
  writers_done.store(true, std::memory_order_release);
  for (size_t r = 0; r < reader_count; ++r) {
    threads[writer_count + r].join();
  }

  const uint64_t errs = torn_reads.load(std::memory_order_relaxed);
  if (errs != 0) {
    std::cerr << "single_key_churn: " << errs
              << " reads returned a value inconsistent with the key "
              << "(torn or stale)\n";
    return false;
  }
  return true;
}

bool run_for_table(const std::string& table_name, const TestConfig& cfg) {
  std::cout << "[table=" << table_name << "]\n";

  {
    auto table = make_by_name(table_name, 1'000'000);
    if (!table) {
      std::cout << "  skipped (table not available)\n";
      return true;
    }
    if (!single_thread_sanity(*table)) {
      std::cerr << "  failed: single_thread_sanity\n";
      return false;
    }
  }

  {
    auto table = make_by_name(table_name, 4'000'000);
    if (!table) {
      return false;
    }
    if (!concurrent_insert(*table, cfg.threads)) {
      std::cerr << "  failed: concurrent_insert\n";
      return false;
    }
  }

  {
    auto table = make_by_name(table_name, 4'000'000);
    if (!table) {
      return false;
    }
    if (!concurrent_mixed(*table, cfg.threads)) {
      std::cerr << "  failed: concurrent_mixed\n";
      return false;
    }
  }

  if (table_name == "cuckoo_optimistic" || table_name == "hopscotch") {
    {
      auto table = make_by_name(table_name, 2'000'000);
      if (!table) {
        return false;
      }
      if (!displacement_stress(*table, cfg.threads)) {
        std::cerr << "  failed: displacement_stress\n";
        return false;
      }
    }
    {
      auto table = make_by_name(table_name, 1'000'000);
      if (!table) {
        return false;
      }
      if (!inserter_lookup_race(*table)) {
        std::cerr << "  failed: inserter_lookup_race\n";
        return false;
      }
    }
    {
      // Small capacity so the hot insert range (~120K keys) forces the
      // displacement path rather than landing in mostly-empty buckets.
      auto table = make_by_name(table_name, 200'000);
      if (!table) {
        return false;
      }
      if (!multi_writer_displacement(*table, cfg.threads, table_name)) {
        std::cerr << "  failed: multi_writer_displacement\n";
        return false;
      }
    }
  }

  if (table_name == "hopscotch") {
    // Small capacity so the 600K combined inserts push load factor high
    // enough that the bubble-up and displacement-budget paths fire.
    // Hopscotch rounds num_buckets up to next_pow2(initial_capacity * 2),
    // so capacity 200K -> 512K buckets -> 600K targeted inserts > 100%
    // load -> many must fail. The test itself asserts at least one cascade
    // insert must fail; if that ever stops holding, bump this down further
    // or reduce cascade count.
    auto table = make_by_name(table_name, 200'000);
    if (!table) {
      return false;
    }
    if (!hopscotch_bubble_race(*table, cfg.threads)) {
      std::cerr << "  failed: hopscotch_bubble_race\n";
      return false;
    }
  }

  // read_heavy_mixed exercises the lookup path under 16 concurrent threads.
  // Write-heavy mixes (e.g. single_key_churn, concurrent_mixed) can miss
  // read-side visibility bugs; this block catches them.
  {
    auto table = make_by_name(table_name, 1'000'000);
    if (!table) {
      return false;
    }
    if (!read_heavy_mixed(*table, 16)) {
      std::cerr << "  failed: read_heavy_mixed\n";
      return false;
    }
  }

  // single_key_churn exercises the remove + reinsert race on the same keys.
  // Runs against every table so we catch torn reads or staleness on the
  // simple (lookup, remove, insert) triple regardless of design.
  {
    auto table = make_by_name(table_name, 1'000'000);
    if (!table) {
      return false;
    }
    if (!single_key_churn(*table, cfg.threads)) {
      std::cerr << "  failed: single_key_churn\n";
      return false;
    }
  }

  std::cout << "  pass\n";
  return true;
}

}  // namespace
}  // namespace ee361c

int main(int argc, char** argv) {
  try {
    const ee361c::TestConfig cfg = ee361c::parse_args(argc, argv);
    size_t failures = 0;
    for (size_t r = 0; r < cfg.repeats; ++r) {
      std::cout << "== repeat " << (r + 1) << "/" << cfg.repeats << " ==\n";
      for (const auto& name : cfg.tables) {
        if (!ee361c::run_for_table(name, cfg)) {
          std::cerr << "correctness_test failed for table: " << name << " (repeat " << (r + 1)
                    << ")\n";
          ++failures;
        }
      }
    }
    if (failures > 0) {
      std::cerr << "correctness_test: total failing tables = " << failures << "\n";
      return 1;
    }
    std::cout << "all correctness tests passed\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "correctness_test error: " << ex.what() << "\n";
    return 1;
  }
}
