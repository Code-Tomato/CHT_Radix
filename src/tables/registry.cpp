// registry.cpp
// Dispatches --table=<name> to the right factory function.

#include "hash_table.hpp"

namespace ee361c {

std::unique_ptr<HashTable> make_by_name(const std::string& name,
                                        size_t initial_capacity) {
  if (name == "stub")             return make_stub(initial_capacity);
  if (name == "chaining_coarse")  return make_chaining_coarse(initial_capacity);
  if (name == "chaining_fine")    return make_chaining_fine(initial_capacity);
  if (name == "cuckoo_optimistic") return make_cuckoo_optimistic(initial_capacity);
  if (name == "cuckoo_striped")   return make_cuckoo_striped(initial_capacity);
  if (name == "hopscotch")        return make_hopscotch(initial_capacity);
  if (name == "radix_tree_coarse") return make_radix_tree_coarse(initial_capacity);
  if (name == "radix_tree_fine")   return make_radix_tree_fine(initial_capacity);
  return nullptr;
}

}  // namespace ee361c