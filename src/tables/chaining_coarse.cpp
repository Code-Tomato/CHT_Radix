#include "hash_table.hpp"

#include <atomic>
#include <mutex>
#include <vector>

#include "hash_utils.hpp"

namespace ee361c {

namespace {

struct Node {
  Key key;
  Value value;
  Node* next;
};

class ChainingCoarse : public HashTable {
public:
  explicit ChainingCoarse(size_t initial_capacity)
      : num_buckets_(next_pow2(initial_capacity)),
        bucket_mask_(num_buckets_ - 1),
        buckets_(num_buckets_, nullptr) {}

  ~ChainingCoarse() override {
    for (auto* head : buckets_) {
      while (head) {
        Node* next = head->next;
        delete head;
        head = next;
      }
    }
  }

  bool insert(Key key, Value value) override {
    std::lock_guard<std::mutex> lock(mutex_);
    const size_t idx = mix64(key) & bucket_mask_;
    for (Node* n = buckets_[idx]; n; n = n->next) {
      if (n->key == key) {
        return false;
      }
    }
    buckets_[idx] = new Node{key, value, buckets_[idx]};
    size_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  bool lookup(Key key, Value& out) override {
    std::lock_guard<std::mutex> lock(mutex_);
    const size_t idx = mix64(key) & bucket_mask_;
    for (Node* n = buckets_[idx]; n; n = n->next) {
      if (n->key == key) {
        out = n->value;
        return true;
      }
    }
    out = kNoValue;
    return false;
  }

  bool remove(Key key) override {
    std::lock_guard<std::mutex> lock(mutex_);
    const size_t idx = mix64(key) & bucket_mask_;
    Node** prev_next = &buckets_[idx];
    for (Node* n = *prev_next; n; prev_next = &n->next, n = n->next) {
      if (n->key == key) {
        *prev_next = n->next;
        delete n;
        size_.fetch_sub(1, std::memory_order_relaxed);
        return true;
      }
    }
    return false;
  }

  size_t size() const override { return size_.load(std::memory_order_relaxed); }
  std::string name() const override { return "chaining_coarse"; }

private:
  size_t num_buckets_;
  size_t bucket_mask_;
  std::vector<Node*> buckets_;
  mutable std::mutex mutex_;
  std::atomic<size_t> size_{0};
};

}  // namespace

std::unique_ptr<HashTable> make_chaining_coarse(size_t initial_capacity) {
  return std::make_unique<ChainingCoarse>(initial_capacity);
}

}  // namespace ee361c
