find_package(Folly REQUIRED)

add_executable(folly_ahm ahm.cc)
target_link_libraries(folly_ahm Folly::folly)
