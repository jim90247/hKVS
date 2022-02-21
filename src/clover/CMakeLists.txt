set(MITSUME_SRC
  ibsetup.cc
  ibsetup.h
  memcached.cc
  memcached.h
  mitsume.h
  mitsume_benchmark.cc
  mitsume_benchmark.h
  mitsume_clt_test.cc
  mitsume_clt_test.h
  mitsume_clt_thread.cc
  mitsume_clt_thread.h
  mitsume_clt_tool.cc
  mitsume_clt_tool.h
  mitsume_con_alloc.cc
  mitsume_con_alloc.h
  mitsume_con_thread.cc
  mitsume_con_thread.h
  mitsume_macro.cc
  mitsume_macro.h
  mitsume_parameter.h
  mitsume_stat.cc
  mitsume_stat.h
  mitsume_struct.cc
  mitsume_struct.h
  mitsume_tool.cc
  mitsume_tool.h
  mitsume_tool_cache.cc
  mitsume_tool_cache.h
  mitsume_tool_gc.cc
  mitsume_tool_gc.h
  mitsume_util.cc
  op_counter.cc
  op_counter.h
  third_party_lru.h
)

add_library(mitsume ${MITSUME_SRC})
target_link_libraries(mitsume ibverbs rt Threads::Threads ${CMAKE_DL_LIBS} memcached numa papi Boost::system Boost::coroutine Boost::thread concurrentqueue::concurrentqueue Folly::folly)
# set LTO
# set_property(TARGET mitsume PROPERTY INTERPROCEDURAL_OPTIMIZATION True)

set(CLOVER_SRC
  client.h
  client.cc
  server.h
  server.cc
  memory.h
  memory.cc
  init.cc
)
add_executable(clover ${CLOVER_SRC})
target_link_libraries(clover mitsume)

install(TARGETS clover)
