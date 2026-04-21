#include "hash_table.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>

namespace ee361c {

namespace {

// Design journey (documented for reviewers and the presentation):
//
//   v1: per-node std::shared_mutex + hand-over-hand (shared locks on read,
//       exclusive on write). Correct, but measured ~8x SLOWER than the
//       coarse global-mutex variant at 8 threads on 95%-read shared-prefix.
//       Root-of-tree exclusive-on-write and per-node shared-lock atomics
//       produced catastrophic cache-line contention on libc++ (macOS).
//
//   v2: per-node std::mutex + hand-over-hand (plain). Even worse (~13x
//       slower than coarse) — root is always locked first, so all threads
//       serialize at root regardless of lock granularity. Per-node mutex
//       just adds overhead on top of a global-mutex-equivalent root.
//
//   v3 (this file): lock-free atomic walk + per-leaf mutex.
//       - children[] is std::array<std::atomic<Node*>, 256>. Readers and
//         writers walk the tree by atomic pointer loads. Zero locks during
//         the walk — no root serialization.
//       - Creating a new child uses compare-and-swap (CAS) on the parent
//         slot. Loser of the race frees its candidate and uses the winner.
//       - The leaf's has_value / value fields are protected by a plain
//         std::mutex on the leaf node only. Only ops that reach the same
//         leaf contend; different leaves are fully parallel.
//       - Nodes are NEVER freed during concurrent operation — only in the
//         tree's destructor after all threads have joined. This is what
//         makes the lock-free pointer walk safe without SMR/RCU/epochs.
class RadixTreeFine : public HashTable {
  struct Node {
    std::array<std::atomic<Node*>, 256> children{};
    mutable std::mutex value_mu;
    bool has_value = false;
    Value value = 0;

    Node() {
      for (auto& slot : children) {
        slot.store(nullptr, std::memory_order_relaxed);
      }
    }

    // Recursive destructor frees the entire subtree. Called only when the
    // owning Node is destroyed, which happens exclusively at tree teardown
    // after all worker threads have joined — no concurrent access.
    ~Node() {
      for (auto& slot : children) {
        Node* child = slot.load(std::memory_order_relaxed);
        if (child) {
          delete child;
        }
      }
    }
  };

public:
  explicit RadixTreeFine(size_t /*initial_capacity*/)
      : root_(std::make_unique<Node>()) {}

  bool insert(Key key, Value value) override {
    Node* cur = root_.get();
    for (int i = 7; i >= 0; --i) {
      const uint8_t byte = static_cast<uint8_t>((key >> (i * 8)) & 0xFFu);
      Node* next = cur->children[byte].load(std::memory_order_acquire);
      if (!next) {
        // Attempt to install a new child via CAS. If we lose the race,
        // delete our candidate and use whatever the winner installed.
        Node* candidate = new Node();
        Node* expected = nullptr;
        if (cur->children[byte].compare_exchange_strong(
                expected, candidate, std::memory_order_release,
                std::memory_order_acquire)) {
          node_count_.fetch_add(1, std::memory_order_relaxed);
          next = candidate;
        } else {
          delete candidate;
          next = expected;
        }
      }
      cur = next;
    }
    std::lock_guard<std::mutex> lock(cur->value_mu);
    if (cur->has_value) {
      return false;
    }
    cur->has_value = true;
    cur->value = value;
    key_count_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  bool lookup(Key key, Value& out) override {
    Node* cur = root_.get();
    for (int i = 7; i >= 0; --i) {
      const uint8_t byte = static_cast<uint8_t>((key >> (i * 8)) & 0xFFu);
      Node* next = cur->children[byte].load(std::memory_order_acquire);
      if (!next) {
        out = kNoValue;
        return false;
      }
      cur = next;
    }
    std::lock_guard<std::mutex> lock(cur->value_mu);
    if (!cur->has_value) {
      out = kNoValue;
      return false;
    }
    out = cur->value;
    return true;
  }

  bool remove(Key key) override {
    Node* cur = root_.get();
    for (int i = 7; i >= 0; --i) {
      const uint8_t byte = static_cast<uint8_t>((key >> (i * 8)) & 0xFFu);
      Node* next = cur->children[byte].load(std::memory_order_acquire);
      if (!next) return false;
      cur = next;
    }
    std::lock_guard<std::mutex> lock(cur->value_mu);
    if (!cur->has_value) return false;
    cur->has_value = false;
    cur->value = 0;
    key_count_.fetch_sub(1, std::memory_order_relaxed);
    return true;
  }

  size_t size() const override {
    return key_count_.load(std::memory_order_relaxed);
  }

  std::string name() const override { return "radix_tree_fine"; }

  size_t node_count_snapshot() const override {
    return node_count_.load(std::memory_order_relaxed);
  }

private:
  std::unique_ptr<Node> root_;
  std::atomic<size_t> key_count_{0};
  std::atomic<size_t> node_count_{1};
};

}  // namespace

std::unique_ptr<HashTable> make_radix_tree_fine(size_t initial_capacity) {
  return std::make_unique<RadixTreeFine>(initial_capacity);
}

}  // namespace ee361c
