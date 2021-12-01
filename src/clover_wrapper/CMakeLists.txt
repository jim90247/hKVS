add_library(clover_wrapper cn.h cn.cc)
target_link_libraries(clover_wrapper PUBLIC gflags ibverbs mitsume)
