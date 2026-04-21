#pragma once

#include <cstddef>
#include <cstdint>

namespace ee361c {

inline uint64_t mix64(uint64_t x) {
  x ^= x >> 30;
  x *= 0xbf58476d1ce4e5b9ULL;
  x ^= x >> 27;
  x *= 0x94d049bb133111ebULL;
  x ^= x >> 31;
  return x;
}

inline uint64_t mix64_alt(uint64_t x) {
  x ^= x >> 33;
  x *= 0xff51afd7ed558ccdULL;
  x ^= x >> 33;
  x *= 0xc4ceb9fe1a85ec53ULL;
  x ^= x >> 33;
  return x;
}

inline size_t next_pow2(size_t x) {
  if (x <= 1) {
    return 1;
  }
  return size_t{1} << (64 - __builtin_clzll(x - 1));
}

}  // namespace ee361c
