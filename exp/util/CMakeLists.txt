add_executable(lru_coverage lru_coverage.cc)
target_link_libraries(lru_coverage glog gflags zipfian lru_records)
