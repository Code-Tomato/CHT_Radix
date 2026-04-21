cmake_minimum_required(VERSION 3.15)
project(ee361c_hashtables CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-Wall -Wextra -Wpedantic)
add_compile_options($<$<CONFIG:Release>:-O3>)
add_compile_options($<$<CONFIG:Release>:-march=native>)

find_package(Threads REQUIRED)

include_directories(include)

add_library(ee361c_harness
  src/bench/workload.cpp
  src/bench/thread_pool.cpp
  src/bench/histogram.cpp
)
target_link_libraries(ee361c_harness PUBLIC Threads::Threads)

add_library(ee361c_tables
  src/tables/registry.cpp
  src/tables/stub.cpp
  src/tables/chaining_coarse.cpp
  src/tables/chaining_fine.cpp
  src/tables/cuckoo_optimistic.cpp
  src/tables/cuckoo_striped.cpp
  src/tables/hopscotch.cpp
  src/tables/radix_tree_coarse.cpp
  src/tables/radix_tree_fine.cpp
)
target_link_libraries(ee361c_tables PUBLIC Threads::Threads)

add_executable(bench src/bench/main.cpp)
target_link_libraries(bench PRIVATE ee361c_harness ee361c_tables)

add_executable(correctness_test src/tests/correctness_test.cpp)
target_link_libraries(correctness_test PRIVATE ee361c_harness ee361c_tables)

option(USE_TSAN "Enable ThreadSanitizer for executables" OFF)
if(USE_TSAN)
  target_compile_options(bench PRIVATE -fsanitize=thread -g)
  target_link_options(bench PRIVATE -fsanitize=thread)
  target_compile_options(correctness_test PRIVATE -fsanitize=thread -g)
  target_link_options(correctness_test PRIVATE -fsanitize=thread)
endif()
