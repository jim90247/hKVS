set(MICA_SRC
  city.c
  city.h
  citycrc.h
  mica.c
  mica.h
)

add_library(mica ${MICA_SRC})
target_include_directories(mica PRIVATE ../libhrd)
target_link_libraries(mica ibverbs rt Threads::Threads memcached numa libhrd)

add_executable(mica_test test.c)
target_include_directories(mica_test PRIVATE ../libhrd ..)
target_link_libraries(mica_test ibverbs rt dl Threads::Threads mica)
