#include "histogram.hpp"

#include <algorithm>

namespace ee361c {

size_t Histogram::bin_for(uint64_t nanos) {
  if (nanos < kLinearMaxNs) {
    return static_cast<size_t>(nanos / kLinearBinNs);
  }
  // Log2 zone: bin i covers [kLinearMaxNs * 2^i, kLinearMaxNs * 2^(i+1)).
  const int lg = 63 - __builtin_clzll(nanos / kLinearMaxNs);
  const size_t log_idx = static_cast<size_t>(lg);
  const size_t clamped = std::min(log_idx, kLog2Bins - 1);
  return kLinearBins + clamped;
}

uint64_t Histogram::bin_lo(size_t idx) {
  if (idx < kLinearBins) {
    return static_cast<uint64_t>(idx) * kLinearBinNs;
  }
  const size_t log_idx = idx - kLinearBins;
  return kLinearMaxNs << log_idx;
}

uint64_t Histogram::bin_hi(size_t idx) {
  if (idx < kLinearBins) {
    return static_cast<uint64_t>(idx + 1) * kLinearBinNs;
  }
  const size_t log_idx = idx - kLinearBins;
  return kLinearMaxNs << (log_idx + 1);
}

void Histogram::record(uint64_t nanos) {
  const size_t idx = bin_for(nanos);
  ++bins_[idx];
  if (nanos > max_sample_) {
    max_sample_ = nanos;
  }
}

void Histogram::merge(const Histogram& other) {
  for (size_t i = 0; i < kNumBins; ++i) {
    bins_[i] += other.bins_[i];
  }
  if (other.max_sample_ > max_sample_) {
    max_sample_ = other.max_sample_;
  }
}

uint64_t Histogram::count() const {
  uint64_t total = 0;
  for (const auto bin_count : bins_) {
    total += bin_count;
  }
  return total;
}

uint64_t Histogram::percentile(double p) const {
  if (p < 0.0) {
    p = 0.0;
  } else if (p > 100.0) {
    p = 100.0;
  }
  const uint64_t total = count();
  if (total == 0) {
    return 0;
  }

  // Target rank in [1, total] using the nearest-rank convention.
  const double rank_d = (p / 100.0) * static_cast<double>(total);
  uint64_t target = static_cast<uint64_t>(rank_d);
  if (target < 1) {
    target = 1;
  }
  if (target > total) {
    target = total;
  }

  uint64_t cumulative = 0;
  for (size_t i = 0; i < kNumBins; ++i) {
    const uint64_t bin_count = bins_[i];
    if (bin_count == 0) {
      continue;
    }
    if (cumulative + bin_count >= target) {
      const uint64_t lo = bin_lo(i);
      const uint64_t hi = bin_hi(i);
      const uint64_t position_in_bin = target - cumulative;  // 1..bin_count
      const double frac =
          (static_cast<double>(position_in_bin) - 0.5) /
          static_cast<double>(bin_count);
      const double interpolated =
          static_cast<double>(lo) + frac * static_cast<double>(hi - lo);
      return static_cast<uint64_t>(interpolated);
    }
    cumulative += bin_count;
  }
  return bin_lo(kNumBins - 1);
}

}  // namespace ee361c
