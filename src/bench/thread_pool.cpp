#include "thread_pool.hpp"

#include <utility>

namespace ee361c {

ThreadPool::ThreadPool(size_t num_threads) {
  if (num_threads == 0) {
    num_threads = 1;
  }
  workers_.reserve(num_threads);
  for (size_t i = 0; i < num_threads; ++i) {
    workers_.emplace_back([this, i] { worker_loop(i); });
  }
}

ThreadPool::~ThreadPool() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_ = true;
  }
  cv_start_.notify_all();
  for (auto& worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
}

void ThreadPool::run(std::function<void(size_t)> fn) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    task_ = std::move(fn);
    remaining_ = workers_.size();
    ++generation_;
  }

  cv_start_.notify_all();

  std::unique_lock<std::mutex> lock(mutex_);
  cv_done_.wait(lock, [this] { return remaining_ == 0; });
}

void ThreadPool::worker_loop(size_t thread_id) {
  size_t local_generation = 0;
  for (;;) {
    std::function<void(size_t)> task;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_start_.wait(lock, [this, local_generation] {
        return stop_ || generation_ != local_generation;
      });
      if (stop_) {
        return;
      }
      local_generation = generation_;
      task = task_;
    }

    task(thread_id);

    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (--remaining_ == 0) {
        cv_done_.notify_one();
      }
    }
  }
}

}  // namespace ee361c
