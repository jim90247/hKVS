set(LIBHRD_SRC
  hrd.h
  hrd_conn.c
  hrd_sizes.h
  hrd_util.c
)

add_library(libhrd ${LIBHRD_SRC})
target_link_libraries(libhrd ibverbs rt Threads::Threads memcached numa)
