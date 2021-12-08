add_executable(combined_worker worker.cc herd_main.h)
target_link_libraries(combined_worker mica mitsume glog libhrd gflags clover_wrapper)

add_executable(combined_client client.cc herd_main.h)
target_link_libraries(combined_client mica mitsume glog libhrd gflags zipfian clover_wrapper)

install(TARGETS combined_worker combined_client)
