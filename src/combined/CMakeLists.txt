add_executable(combined_worker worker.cc herd_main.h)
target_link_libraries(combined_worker mica mitsume glog libhrd gflags clover_wrapper)

add_executable(combined_client client.cc herd_main.h)
target_link_libraries(combined_client mica glog libhrd gflags zipfian)

install(TARGETS combined_worker combined_client)
