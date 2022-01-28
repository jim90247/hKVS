#include <folly/AtomicHashMap.h>
#include <gflags/gflags.h>

#include <iostream>
using AHM = folly::AtomicHashMap<unsigned long, unsigned long>;

unsigned long Traverse(AHM& ahm, unsigned long key) {
  while (true) {
    auto it = ahm.find(key);
    if (it == ahm.end() || it->second == 0UL) {
      return key;
    } else {
      key = it->second;
    }
  }
}

int main() {
  folly::AtomicHashMap<unsigned long, unsigned long> ahm(3);
  ahm.insert(std::make_pair(1UL, 2UL));
  ahm.insert(std::make_pair(2UL, 3UL));
  ahm.insert(std::make_pair(3UL, 0UL));
  std::cout << "Traverse(1) = " << Traverse(ahm, 1) << std::endl;
  std::cout << "Traverse(4) = " << Traverse(ahm, 4) << std::endl;
  ahm.erase(2);
  std::cout << "Traverse(1) = " << Traverse(ahm, 1) << std::endl;
  return 0;
}
