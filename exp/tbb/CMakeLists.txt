find_package(TBB)

add_executable(tbb_test tbb.cc)
target_link_libraries(tbb_test TBB::tbb)
