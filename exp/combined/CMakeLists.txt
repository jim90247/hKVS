add_executable(req_queue_bench req_queue.cc)
target_link_libraries(req_queue_bench Threads::Threads concurrentqueue::concurrentqueue gflags glog Folly::folly)
