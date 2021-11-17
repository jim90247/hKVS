set(HERD_SRC
  client.c
  main.c
  main.h
  master.c
  worker.c
)

add_executable(herd ${HERD_SRC})
target_include_directories(herd PRIVATE ../mica ../libhrd)
target_link_libraries(herd ibverbs rt Threads::Threads memcached numa mica libhrd)

install(TARGETS herd)
