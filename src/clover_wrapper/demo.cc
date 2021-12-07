#include <gflags/gflags.h>
#include <glog/logging.h>

#include <iostream>
#include <string>

#include "cn.h"

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  CloverComputeNodeWrapper clover_node(1);
  clover_node.Initialize();
  CloverCnThreadWrapper clover_thread(std::ref(clover_node), 0);
  LOG(INFO) << "Done initialization";

  constexpr size_t kBufSize = 4096;
  char buf[kBufSize] = {0};
  uint32_t len = 0;
  string cmd, value;
  mitsume_key key = 123;
  int rc = 0;

  /**
   * @brief An interactive interface to receive and process operations from
   * command line input.
   */
  auto interactive_cmd = [&]() {
    const string prompt = "<open|read|write> <key (8-byte integer)> [value] $ ";
    cout << prompt;
    while (cin >> cmd) {
      if (cmd == "open") {
        cin >> key >> value;
        rc = clover_thread.InsertKVPair(key, value.c_str(), value.length());
        if (rc == MITSUME_SUCCESS) {
          LOG(INFO) << "Successfully insert " << key << ": " << value << " ("
                    << value.length() << " bytes)";
        }
      } else if (cmd == "read") {
        cin >> key;
        rc = clover_thread.ReadKVPair(key, buf, &len, kBufSize);
        if (rc == MITSUME_SUCCESS) {
          value = string(buf);
          LOG(INFO) << "Successfully read " << key << ": " << value << " ("
                    << len << " bytes)";
        }
      } else if (cmd == "write") {
        cin >> key >> value;
        rc = clover_thread.WriteKVPair(key, value.c_str(), value.length());
        if (rc == MITSUME_SUCCESS) {
          LOG(INFO) << "Successfully update " << key << ": " << value << " ("
                    << value.length() << " bytes)";
        }
      }
      cout << prompt;
    }
    cout << endl;
  };
  interactive_cmd();
  return 0;
}