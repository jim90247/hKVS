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

void Insert(HashMap& mp, Key key, Value value) {
  if (mp.insert(std::make_pair(key, value))) {
    std::cout << "Insert " << key << " succeed (value=" << value << ")" << std::endl;
  } else {
    std::cout << "Insert " << key << " failed" << std::endl;
  }
}

int main() {
  HashMap mp;
  Insert(mp, 10, 100);
  Insert(mp, 10, 200);
  LookUp(mp, 10);
  LookUp(mp, 100);
  mp.erase(10);
  LookUp(mp, 10);
  return 0;
}
