#include "affinity.h"

#include <folly/logging/xlog.h>

void SetAffinity(std::thread& t, unsigned int core) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);
  auto rc =
      pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    XLOGF(ERR, "Failed to set affinity to core {}!", core);
  }
}
