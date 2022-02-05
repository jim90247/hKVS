#include <absl/container/flat_hash_map.h>

#include <iostream>

int main() {
  absl::flat_hash_map<unsigned long, unsigned long> hmp;
  hmp.insert(std::make_pair(1UL, 2UL));
  std::cout << hmp[1] << std::endl;
  hmp.insert_or_assign(1UL, 3UL);
  std::cout << hmp[1] << std::endl;
  return 0;
}