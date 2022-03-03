set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_subdirectory(libhrd)
add_subdirectory(mica)
add_subdirectory(herd)

add_subdirectory(clover)
add_subdirectory(clover_wrapper)

add_subdirectory(util)

add_subdirectory(combined)
add_subdirectory(separated)
