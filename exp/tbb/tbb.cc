#include <oneapi/tbb/concurrent_hash_map.h>

#include <iostream>

using Key = int;
using Value = int;
using HashMap = tbb::concurrent_hash_map<Key, Value>;

void LookUp(const HashMap& mp, Key key) {
  HashMap::const_accessor accessor;
  if (mp.find(accessor, key)) {
    std::cout << accessor->first << " found: " << accessor->second << std::endl;
  } else {
    std::cout << key << " not found" << std::endl;
  }
}

int main() {
  HashMap mp;
  mp.insert(std::make_pair(10, 100));
  LookUp(mp, 10);
  LookUp(mp, 100);
  mp.erase(10);
  LookUp(mp, 10);
  return 0;
}
