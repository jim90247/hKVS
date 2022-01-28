#include <folly/concurrency/ConcurrentHashMap.h>
#include <gflags/gflags.h>

#include <iostream>
using CHM = folly::ConcurrentHashMap<unsigned long, unsigned long>;

unsigned long Traverse(CHM& chm, unsigned long key) {
  while (true) {
    auto it = chm.find(key);
    if (it == chm.end() || it->second == 0UL) {
      return key;
    } else {
      key = it->second;
    }
  }
}

int main() {
  CHM chm(3);
  chm.insert(std::make_pair(1UL, 2UL));
  chm.insert(std::make_pair(2UL, 3UL));
  chm.insert(std::make_pair(3UL, 0UL));
  std::cout << "Traverse(1) = " << Traverse(chm, 1) << std::endl;
  std::cout << "Traverse(4) = " << Traverse(chm, 4) << std::endl;
  chm.erase(2);
  std::cout << "Traverse(1) = " << Traverse(chm, 1) << std::endl;
  return 0;
}
