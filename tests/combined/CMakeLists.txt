add_executable(combined_tests
  combined_test_main.cc
  req_submitter_test.cc
)
target_link_libraries(combined_tests Catch2::Catch2 combined_clover_worker)
catch_discover_tests(combined_tests)
