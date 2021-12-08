add_library(clover_wrapper cn.h cn.cc)
target_link_libraries(clover_wrapper PUBLIC gflags ibverbs mitsume)

add_executable(clover_wrapper_demo demo.cc)
target_link_libraries(clover_wrapper_demo PUBLIC clover_wrapper glog gflags)
install(TARGETS clover_wrapper_demo)
