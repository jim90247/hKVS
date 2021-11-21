add_executable(cn_try client.cc)
target_link_libraries(cn_try mitsume glog)

install(TARGETS cn_try)
