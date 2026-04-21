#include "hash_table.hpp"

#include <atomic>
#include <memory>
#include <mutex>

#include "hash_utils.hpp"

namespace ee361c {

namespace {

struct Node {
  Key key;
  Value value;
  Node* next;
};

struct alignas(64) Bucket {
  std::mutex mutex;
  Node* head = nullptr;
};
static_assert(sizeof(Bucket) == 64 || sizeof(Bucket) == 128,
              "Bucket should be one or two cache lines");

class ChainingFine : public HashTable {
public:
  explicit ChainingFine(size_t initial_capacity)
      : num_buckets_(next_pow2(initial_capacity)),
        bucket_mask_(num_buckets_ - 1),
        buckets_(std::make_unique<Bucket[]>(num_buckets_)) {}

  ~ChainingFine() override {
    for (size_t i = 0; i < num_buckets_; ++i) {
      Node* n = buckets_[i].head;
      while (n) {
        Node* next = n->next;
        delete n;
        n = next;
      }
    }
  }

  bool insert(Key key, Value value) override {
    const size_t idx = mix64(key) & bucket_mask_;
    Bucket& b = buckets_[idx];
    std::lock_guard<std::mutex> lock(b.mutex);
    for (Node* n = b.head; n; n = n->next) {
      if (n->key == key) {
        return false;
      }
    }
    b.head = new Node{key, value, b.head};
    size_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  bool lookup(Key key, Value& out) override {
    const size_t idx = mix64(key) & bucket_mask_;
    Bucket& b = buckets_[idx];
    std::lock_guard<std::mutex> lock(b.mutex);
    for (Node* n = b.head; n; n = n->next) {
      if (n->key == key) {
        out = n->value;
        return true;
      }
    }
    out = kNoValue;
    return false;
  }

  bool remove(Key key) override {
    const size_t idx = mix64(key) & bucket_mask_;
    Bucket& b = buckets_[idx];
    std::lock_guard<std::mutex> lock(b.mutex);
    Node** prev_next = &b.head;
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
  std::string name() const override { return "chaining_fine"; }

private:
  size_t num_buckets_;
  size_t bucket_mask_;
  std::unique_ptr<Bucket[]> buckets_;
  std::atomic<size_t> size_{0};
};

}  // namespace

std::unique_ptr<HashTable> make_chaining_fine(size_t initial_capacity) {
  return std::make_unique<ChainingFine>(initial_capacity);
}

}  // namespace ee361c
