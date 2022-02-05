find_package(absl REQUIRED)

add_executable(absl_flat_hmap flat_hash_map.cc)
target_link_libraries(absl_flat_hmap absl::flat_hash_map)
