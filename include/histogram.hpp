#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace ee361c {

// Precision latency histogram.
//
// Two-zone layout to give meaningful sub-microsecond percentiles without a
// heavy dependency:
//
//   Linear zone: [0, kLinearMaxNs) in kLinearBinNs-wide bins. Covers the
//   common sub-microsecond latency range at a true ~64 ns resolution.
//
//   Log2 zone:   [kLinearMaxNs, ~4 s) in power-of-two bins. Tail-friendly and
//   keeps the total bin count small.
//
// Percentiles linearly interpolate within the bin that contains the target
// rank, so p50/p99/p999 are actual percentile estimates rather than bin
// floors. Inputs and outputs stay in nanoseconds.
class Histogram {
 public:
  static constexpr uint64_t kLinearBinNs = 64;
  static constexpr size_t kLinearBins = 256;
  static constexpr uint64_t kLinearMaxNs = kLinearBinNs * kLinearBins;
  static constexpr size_t kLog2Bins = 26;  // covers up to ~4 s
  static constexpr size_t kNumBins = kLinearBins + kLog2Bins;

  void record(uint64_t nanos);
  void merge(const Histogram& other);

  uint64_t count() const;
  uint64_t percentile(double p) const;
  uint64_t p50() const { return percentile(50.0); }
  uint64_t p99() const { return percentile(99.0); }
  uint64_t p999() const { return percentile(99.9); }

  // Exact maximum sample observed via record() across this and any merged
  // histograms, in nanoseconds. Not a bin-edge approximation.
  uint64_t max() const { return max_sample_; }

 private:
  static size_t bin_for(uint64_t nanos);
  static uint64_t bin_lo(size_t idx);
  static uint64_t bin_hi(size_t idx);

  std::array<uint64_t, kNumBins> bins_{};
  uint64_t max_sample_ = 0;
};

}  // namespace ee361c
