#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/container/F14Set.h>
#include <folly/logging/xlog.h>
#include <gflags/gflags.h>

#include <atomic>
#include <boost/thread/barrier.hpp>
#include <cmath>
#include <future>
#include <mutex>
#include <random>

#include "separated/herd_client.h"
#include "util/affinity.h"

constexpr int kHerdServerPorts = 1;  // Number of server IB ports
constexpr int kHerdClientPorts = 1;  // Number of client IB ports
constexpr int kHerdBasePortIdx = 0;  // Base IB port index

constexpr unsigned int kTraceLength = 10'000'000;
constexpr unsigned int kWarmUpIter = 3'000'000;

DEFINE_uint32(herd_threads, 24, "Number of HERD client threads");
DEFINE_uint32(herd_mach_id, 0, "HERD machine id");
DEFINE_uint32(bench_secs, 10, "Seconds to run benchmark");
DEFINE_bool(pin_threads, false, "Pin each worker thread to one core");

static std::vector<uint128> GenerateTrace(int seed) {
  std::minstd_rand gen(seed);
  std::uniform_int_distribution<int> dis(0, HERD_NUM_KEYS - 1);
  std::vector<uint128> trace;

  for (size_t i = 0; i < kTraceLength; i++) {
    int plain_key = dis(gen);
    trace.emplace_back(ConvertPlainKeyToHerdKey(plain_key));
  }
  return trace;
}

void HerdWarmUp(HerdClient& cli, const std::vector<uint128>& trace) {
  std::vector<HerdResp> resps;
  unsigned int trace_idx = 0;
  for (unsigned int i = 0; i < kWarmUpIter; i++) {
    auto key = trace[trace_idx];
    while (!cli.PostRequest(key, nullptr, HERD_VALUE_SIZE, true)) {
      cli.GetResponses(resps);
    }

    HRD_MOD_ADD(trace_idx, trace.size());
  }
  cli.GetResponses(resps);
}

void HerdMain(const int global_id, boost::barrier& barrier,
              std::atomic_bool& stop_flag,
              std::promise<unsigned long> tput_prm) {
  auto trace = GenerateTrace(global_id);

  HerdClient cli(global_id, kHerdServerPorts, kHerdClientPorts,
                 kHerdBasePortIdx);
  cli.ConnectToServer();
  HerdWarmUp(cli, trace);

  barrier.wait();

  std::vector<HerdResp> resps;
  unsigned int trace_idx = 0;

  unsigned long completed = 0;

  while (!stop_flag.load(std::memory_order_acquire)) {
    auto key = trace[trace_idx];
    while (!cli.PostRequest(key, nullptr, HERD_VALUE_SIZE, true)) {
      cli.GetResponses(resps);
    }

    HRD_MOD_ADD(trace_idx, trace.size());
    completed++;
  }

  unsigned long tput = completed / FLAGS_bench_secs;
  XLOGF(INFO, "Echo: {:8d} op/s.", tput);
  tput_prm.set_value(tput);
}

void CountDownMain(boost::barrier& barrier, std::atomic_bool& stop_flag) {
  barrier.wait();
  XLOGF(INFO, "Start benchmarking for {} seconds.", FLAGS_bench_secs);
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_bench_secs));
  stop_flag.store(true, std::memory_order_release);
}

int main(int argc, char** argv) {
  {
    auto reg_ip_env = std::getenv("HRD_REGISTRY_IP");
    if (reg_ip_env != nullptr) {
      XLOGF(INFO, "HERD registry: {}", reg_ip_env);
    }
  }
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();

  XLOGF(INFO, "Using {} client threads.", FLAGS_herd_threads);
  XLOGF(INFO, "Value size: {} bytes.", HERD_VALUE_SIZE);

  boost::barrier barrier(FLAGS_herd_threads + 1);
  std::atomic_bool stop_flag = ATOMIC_VAR_INIT(false);

  std::vector<std::thread> threads;
  std::vector<std::future<unsigned long>> echo_tput_futs;

  for (unsigned int i = 0; i < FLAGS_herd_threads; i++) {
    std::promise<unsigned long> prm;
    echo_tput_futs.push_back(prm.get_future());
    threads.emplace_back(HerdMain, FLAGS_herd_mach_id * FLAGS_herd_threads + i,
                         std::ref(barrier), std::ref(stop_flag),
                         std::move(prm));
  }

  if (FLAGS_pin_threads) {
    XLOG(INFO, "Pin benchmark threads to specific CPU core");
    unsigned int core = 0;
    for (auto& t : threads) {
      SetAffinity(t, core++);
    }
  }
  threads.emplace_back(CountDownMain, std::ref(barrier), std::ref(stop_flag));

  unsigned long herd_tput = 0;
  for (auto& fut : echo_tput_futs) {
    herd_tput += fut.get();
  }
  XLOGF(INFO, "Echo: {} op/s", herd_tput);
  for (auto& t : threads) {
    t.join();
  }
  return 0;
}
