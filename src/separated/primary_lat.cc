#include "combined/herd_main.h"

int main(int argc, char **argv) {
  FLAGS_colorlogtostderr = true;
  FLAGS_alsologtostderr = true;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  return 0;
}
