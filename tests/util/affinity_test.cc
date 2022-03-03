#include "util/affinity.h"

#include <boost/thread/barrier.hpp>
#include <catch2/catch.hpp>
#include <future>

static void ThreadMain(boost::barrier& barrier, std::promise<int> core_prm) {
  barrier.wait();
  core_prm.set_value(sched_getcpu());
}

TEST_CASE("CPU affinity") {
  constexpr int kCpuId = 2;
  boost::barrier barrier(2);
  std::promise<int> core_prm;
  auto core = core_prm.get_future();

  std::thread t(ThreadMain, std::ref(barrier), std::move(core_prm));
  SetAffinity(t, kCpuId);
  barrier.wait();
  t.join();

  REQUIRE(core.get() == kCpuId);
}
