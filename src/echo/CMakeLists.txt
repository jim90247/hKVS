add_executable(echo_server main.c main.h master.c worker.c)
target_include_directories(echo_server PRIVATE ../mica ../libhrd)
target_link_libraries(echo_server Threads::Threads ibverbs rt memcached numa mica libhrd)

add_executable(echo_client client.cc)
target_link_libraries(echo_client separated_herd_client affinity)
