set(COMBINED_COMMON_LIB
  gflags
  glog
  mitsume
  clover_wrapper
  Folly::folly
)

add_library(combined_clover_worker clover_worker.h clover_worker.cc req_submitter.h req_submitter.cc)
target_link_libraries(combined_clover_worker ${COMBINED_COMMON_LIB} concurrentqueue::concurrentqueue)

add_executable(combined_worker worker.cc herd_main.h)
target_link_libraries(combined_worker ${COMBINED_COMMON_LIB} mica libhrd combined_clover_worker lru_records affinity)

add_executable(combined_client client.cc herd_main.h)
target_link_libraries(combined_client ${COMBINED_COMMON_LIB} mica libhrd combined_clover_worker zipfian Boost::thread)

install(TARGETS combined_worker combined_client)
