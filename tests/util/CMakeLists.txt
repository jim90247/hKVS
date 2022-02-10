add_executable(util_tests
  util_test_main.cc
  lru_records_test.cc
)
target_link_libraries(util_tests Catch2::Catch2 lru_records)
catch_discover_tests(util_tests)
