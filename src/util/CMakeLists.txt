add_library(zipfian zipfian_generator.h zipfian_generator.cc)

add_library(lru_records INTERFACE)
target_link_libraries(lru_records INTERFACE Folly::folly)

add_library(affinity affinity.h affinity.cc)
target_link_libraries(affinity PUBLIC Threads::Threads PRIVATE Folly::folly)
