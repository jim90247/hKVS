find_package(Folly REQUIRED)

add_executable(folly_ahm ahm.cc)
target_link_libraries(folly_ahm Folly::folly)

add_executable(folly_chm chm.cc)
target_link_libraries(folly_chm Folly::folly)

add_executable(folly_dmpscq dmpscq.cc)
target_link_libraries(folly_dmpscq Folly::folly)
