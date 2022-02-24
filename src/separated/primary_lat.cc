#include <folly/logging/xlog.h>
#include <gflags/gflags.h>

#include "herd_client.h"

DEFINE_int32(server_ports, 1, "Number of server IB ports");
DEFINE_int32(client_ports, 1, "Number of client IB ports");
DEFINE_int32(base_port_index, 0, "Base IB port index");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();

  // check at runtime (instead of compile-time) to allow other codes to compile
  XCHECK_EQ(NUM_CLIENTS, 1);

  {
    auto memcached_ip = std::getenv("HRD_REGISTRY_IP");
    XCHECK_NE(memcached_ip, nullptr);
    XLOGF(INFO, "Memcached server: {}", memcached_ip);
  }
  XLOGF(INFO, "Value size: {}", HERD_VALUE_SIZE);

  HerdClient cli(0, FLAGS_server_ports, FLAGS_client_ports,
                 FLAGS_base_port_index);
  cli.ConnectToServer();

  return 0;
}
