#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace ee361c {

class ThreadPool {
public:
  explicit ThreadPool(size_t num_threads);
  ~ThreadPool();

  void run(std::function<void(size_t)> fn);

  size_t size() const { return workers_.size(); }

private:
  void worker_loop(size_t thread_id);

  std::vector<std::thread> workers_;

  std::mutex mutex_;
  std::condition_variable cv_start_;
  std::condition_variable cv_done_;

  std::function<void(size_t)> task_;
  size_t remaining_ = 0;
  size_t generation_ = 0;
  bool stop_ = false;
};

}  // namespace ee361c
