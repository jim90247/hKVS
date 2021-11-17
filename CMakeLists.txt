cmake_minimum_required(VERSION 3.10)
project(kvs VERSION 0.1)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_BUILD_TYPE RelWithDebInfo)
add_compile_options(-Wall -Wextra)

if (NOT DEBUG)
  add_compile_definitions(NDEBUG)
endif()

set(CMAKE_INSTALL_PREFIX ${PROJECT_SOURCE_DIR})

include_directories(${PROJECT_SOURCE_DIR}/src)

add_subdirectory(src)
