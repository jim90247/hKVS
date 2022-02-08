# cameron314/concurrentqueue
find_package(concurrentqueue REQUIRED)
find_package(Threads REQUIRED)
find_package(Folly REQUIRED)

add_executable(req_queue_bench req_queue.cc)
target_link_libraries(req_queue_bench Threads::Threads concurrentqueue::concurrentqueue gflags glog Folly::folly)
