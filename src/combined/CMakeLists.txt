add_executable(combined_worker worker.cc herd_main.h)
target_link_libraries(combined_worker mitsume glog libhrd)

install(TARGETS combined_worker)
