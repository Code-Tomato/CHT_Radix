#include "hash_table.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>

namespace ee361c {

namespace {

class RadixTreeCoarse : public HashTable {
  struct Node {
    std::array<std::unique_ptr<Node>, 256> children{};
    bool has_value = false;
    Value value = 0;
  };

public:
  // initial_capacity is ignored: a byte-level trie's shape is data-dependent,
  // not hint-driven. Accept the parameter to match the factory contract.
  explicit RadixTreeCoarse(size_t /*initial_capacity*/)
      : root_(std::make_unique<Node>()) {}

  bool insert(Key key, Value value) override {
    std::lock_guard<std::mutex> lock(mutex_);
    Node* cur = root_.get();
    for (int i = 7; i >= 0; --i) {
      const uint8_t byte = static_cast<uint8_t>((key >> (i * 8)) & 0xFFu);
      auto& child = cur->children[byte];
      if (!child) {
        child = std::make_unique<Node>();
        node_count_.fetch_add(1, std::memory_order_relaxed);
      }
      cur = child.get();
    }
    if (cur->has_value) {
      return false;
    }
    cur->has_value = true;
    cur->value = value;
    key_count_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  bool lookup(Key key, Value& out) override {
    std::lock_guard<std::mutex> lock(mutex_);
    Node* cur = root_.get();
    for (int i = 7; i >= 0; --i) {
      const uint8_t byte = static_cast<uint8_t>((key >> (i * 8)) & 0xFFu);
      Node* next = cur->children[byte].get();
      if (!next) {
        out = kNoValue;
        return false;
      }
      cur = next;
    }
    if (!cur->has_value) {
      out = kNoValue;
      return false;
    }
    out = cur->value;
    return true;
  }

  bool remove(Key key) override {
    std::lock_guard<std::mutex> lock(mutex_);
    Node* cur = root_.get();
    for (int i = 7; i >= 0; --i) {
      const uint8_t byte = static_cast<uint8_t>((key >> (i * 8)) & 0xFFu);
      Node* next = cur->children[byte].get();
      if (!next) return false;
      cur = next;
    }
    if (!cur->has_value) return false;
    cur->has_value = false;
    cur->value = 0;
    key_count_.fetch_sub(1, std::memory_order_relaxed);
    // Do NOT prune empty subtrees. MVP simplicity + keeps Node* stable for
    // the forthcoming fine-grained variant, which relies on nodes never
    // disappearing.
    return true;
  }

  size_t size() const override {
    return key_count_.load(std::memory_order_relaxed);
  }

  std::string name() const override { return "radix_tree_coarse"; }

  size_t node_count_snapshot() const override {
    return node_count_.load(std::memory_order_relaxed);
  }

private:
  mutable std::mutex mutex_;
  std::unique_ptr<Node> root_;
  std::atomic<size_t> key_count_{0};
  std::atomic<size_t> node_count_{1};
};

}  // namespace

std::unique_ptr<HashTable> make_radix_tree_coarse(size_t initial_capacity) {
  return std::make_unique<RadixTreeCoarse>(initial_capacity);
}

}  // namespace ee361c
