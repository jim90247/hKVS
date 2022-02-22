set(COMBINED_HDRS ../combined/herd_main.h)

add_executable(client_primary_lat primary_lat.cc ${COMBINED_HDRS})
target_link_libraries(client_primary_lat Folly::folly gflags glog mica libhrd zipfian concurrentqueue::concurrentqueue)

install(TARGETS client_primary_lat)
