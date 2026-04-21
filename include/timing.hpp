#pragma once

#include <chrono>
#include <cstdint>

namespace ee361c {

class Timer {
public:
  Timer() : start_(std::chrono::steady_clock::now()) {}

  void reset() { start_ = std::chrono::steady_clock::now(); }

  double elapsed_seconds() const {
    const auto now = std::chrono::steady_clock::now();
    return std::chrono::duration<double>(now - start_).count();
  }

  uint64_t elapsed_ns() const {
    const auto now = std::chrono::steady_clock::now();
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now - start_).count());
  }

private:
  std::chrono::steady_clock::time_point start_;
};

// Per-operation latency sample in nanoseconds. Backed by std::chrono::steady_clock
// (mach_absolute_time on macOS, CLOCK_MONOTONIC on Linux). Not a cycle counter.
//
// Quantization floor: the clock tick resolution bounds the smallest latency
// you can distinguish. Typical observed floors: ~40-100 ns on macOS
// (mach_absolute_time scaled), ~1-40 ns on Linux CLOCK_MONOTONIC. Any p50 or
// p99 value reported within one tick of the minimum observed latency is at
// the clock's resolution, not the measurement's. The benchmark's precision
// histogram bins at 64 ns; sub-bin percentile interpolation cannot recover
// below-tick resolution.
inline uint64_t sample_ns() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

}  // namespace ee361c
