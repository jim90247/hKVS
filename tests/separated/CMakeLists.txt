add_executable(separated_herd_client_tests
  herd_client_test_main.cc
  herd_client_test.cc
)

target_link_libraries(separated_herd_client_tests
  Catch2::Catch2
  separated_herd_client
)
