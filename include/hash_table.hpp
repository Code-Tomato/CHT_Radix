// hash_table.hpp
// Common interface for all concurrent hash table implementations.
//
// All implementations MUST be safe for concurrent use by multiple threads
// without external synchronization. Individual method preconditions and
// postconditions are documented below.

#pragma once

#include <cstdint>
#include <cstddef>
#include <memory>
#include <string>

namespace ee361c {

// Key and value types. Fixed at 64-bit integers for the full evaluation —
// this matches what libcuckoo, MemC3, and most concurrent hashing papers use,
// avoids variable-length key complications, and keeps bucket layouts simple.
using Key   = uint64_t;
using Value = uint64_t;

// Sentinel value returned for lookup misses. Callers check the bool return,
// not this value — it exists so `out_value` is always in a defined state.
constexpr Value kNoValue = 0;

// Abstract base class. Every implementation subclasses this.
//
// Thread safety: ALL methods must be safe to call concurrently from any
// number of threads without external synchronization.
//
// Memory model: implementations should provide at least sequentially
// consistent semantics for operations on distinct keys. Operations on the
// same key are linearizable.
class HashTable {
public:
  virtual ~HashTable() = default;

  // Insert a key-value pair.
  //   Returns true  if the key was newly inserted.
  //   Returns false if the key already existed (value is NOT updated).
  virtual bool insert(Key key, Value value) = 0;

  // Look up a key.
  //   Returns true  if the key was found; out_value is set to its value.
  //   Returns false if the key was not found; out_value is set to kNoValue.
  virtual bool lookup(Key key, Value& out_value) = 0;

  // Remove a key.
  //   Returns true  if the key was present and is now removed.
  //   Returns false if the key was not present.
  virtual bool remove(Key key) = 0;

  // Approximate size. Implementations may return a stale count under
  // concurrent modification — this is for sanity checks, not correctness.
  virtual size_t size() const = 0;

  // Identifier for logging / output. e.g. "chaining_coarse", "cuckoo_optimistic".
  virtual std::string name() const = 0;

  // Diagnostic: approximate count of internal nodes/buckets allocated.
  // Default returns 0; radix trees override to report allocated-node count.
  // NOT a correctness metric and NOT comparable across structures.
  virtual size_t node_count_snapshot() const { return 0; }
};

// ---------------------------------------------------------------------------
// Factory functions. Each implementer adds theirs here and in the matching .cpp.
// The benchmark harness constructs tables exclusively through these so the
// harness never has to know about concrete types.
//
// initial_capacity is a hint. Implementations may round up to a power of two
// or ignore it, but they should size themselves to avoid resizing during the
// benchmark. No implementation is required to support dynamic resizing.
// ---------------------------------------------------------------------------

std::unique_ptr<HashTable> make_chaining_coarse(size_t initial_capacity);
std::unique_ptr<HashTable> make_chaining_fine(size_t initial_capacity);
std::unique_ptr<HashTable> make_cuckoo_optimistic(size_t initial_capacity);
std::unique_ptr<HashTable> make_cuckoo_striped(size_t initial_capacity);
std::unique_ptr<HashTable> make_hopscotch(size_t initial_capacity);
std::unique_ptr<HashTable> make_stub(size_t initial_capacity);
std::unique_ptr<HashTable> make_radix_tree_coarse(size_t initial_capacity);
std::unique_ptr<HashTable> make_radix_tree_fine(size_t initial_capacity);

// Registry lookup — the harness uses this to resolve --table=<name> flags.
// Returns nullptr if name is unknown.
std::unique_ptr<HashTable> make_by_name(const std::string& name,
                                        size_t initial_capacity);

}  // namespace ee361c