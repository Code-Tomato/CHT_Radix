#include "hash_table.hpp"

#include <mutex>
#include <unordered_map>

namespace ee361c {

class StubTable : public HashTable {
public:
  explicit StubTable(size_t cap) { map_.reserve(cap); }

  bool insert(Key k, Value v) override {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_.emplace(k, v).second;
  }

  bool lookup(Key k, Value& out) override {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = map_.find(k);
    if (it == map_.end()) {
      out = kNoValue;
      return false;
    }
    out = it->second;
    return true;
  }

  bool remove(Key k) override {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_.erase(k) > 0;
  }

  size_t size() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_.size();
  }

  std::string name() const override { return "stub"; }

private:
  mutable std::mutex mutex_;
  std::unordered_map<Key, Value> map_;
};

std::unique_ptr<HashTable> make_stub(size_t cap) {
  return std::make_unique<StubTable>(cap);
}

}  // namespace ee361c
