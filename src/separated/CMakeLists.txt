set(COMBINED_HDRS ../combined/herd_main.h)

set(COMMON_SRCS timing.h)

add_library(separated_herd_client
  herd_client.h
  herd_client.cc
  ${COMBINED_HDRS}
)
target_link_libraries(separated_herd_client
  Folly::folly
  mica
  libhrd
  concurrentqueue::concurrentqueue
)

add_executable(client_primary_lat primary_lat.cc ${COMMON_SRCS})
target_link_libraries(client_primary_lat
  Folly::folly
  fmt::fmt
  gflags
  glog
  separated_herd_client
  zipfian
)

install(TARGETS client_primary_lat)
