find_package(Threads REQUIRED)

add_library(zipfian zipfian_generator.h zipfian_generator.cc)

add_library(twitter_trace twitter_trace_reader.h twitter_trace_reader.cc)
target_link_libraries(twitter_trace PUBLIC Threads::Threads)
