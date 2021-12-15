cmake_minimum_required(VERSION 3.10)
project(kvs VERSION 0.1)

option(BUILD_TESTING "Build unit tests" OFF)
option(DEBUG "Build in debug mode" OFF)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_BUILD_TYPE RelWithDebInfo)
# add -rdynamic
set(CMAKE_ENABLE_EXPORTS ON)
add_compile_options(-Wall -Wextra)

if (NOT DEBUG)
  add_compile_definitions(NDEBUG)
endif()

set(CMAKE_INSTALL_PREFIX ${PROJECT_SOURCE_DIR})

include_directories(${PROJECT_SOURCE_DIR}/src)

add_subdirectory(src)
add_subdirectory(exp)

if (BUILD_TESTING)
  find_package(Catch2 REQUIRED)
  include(CTest)
  include(Catch)
  add_subdirectory(tests)
endif()
