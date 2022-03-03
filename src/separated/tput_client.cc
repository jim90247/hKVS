#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/container/F14Set.h>
#include <folly/logging/xlog.h>
#include <gflags/gflags.h>

#include <atomic>
#include <boost/thread/barrier.hpp>
#include <cmath>
#include <future>
#include <mutex>

#include "clover_wrapper/cn.h"
#include "herd_client.h"
#include "util/affinity.h"
#include "util/zipfian_generator.h"

constexpr int kHerdServerPorts = 1;  // Number of server IB ports
constexpr int kHerdClientPorts = 1;  // Number of client IB ports
constexpr int kHerdBasePortIdx = 0;  // Base IB port index

constexpr int kCloverCoros = 4;

constexpr unsigned int kTraceLength = 10'000'000;
constexpr double kZipfianAlpha = 0.99;
constexpr unsigned int kHerdWarmUpIter = 3'000'000;

DEFINE_uint32(herd_threads, 24, "Number of HERD client threads");
DEFINE_uint32(herd_mach_id, 0, "HERD machine id");
DEFINE_uint32(clover_threads, 16, "Number of Clover client threads");
DEFINE_uint32(update_pct, 5, "Percentage of update operation");
DEFINE_uint32(bench_secs, 40, "Seconds to run benchmark");
DEFINE_bool(pin_threads, false, "Pin each worker thread to one core");

folly::ConcurrentHashMap<mitsume_key, bool> offloaded;

static std::vector<uint128> GenerateTrace(int seed) {
  ZipfianGenerator gen(HERD_NUM_KEYS, kZipfianAlpha, seed);
  std::vector<uint128> trace;

  for (size_t i = 0; i < kTraceLength; i++) {
    int plain_key = gen.GetNumber();
    trace.emplace_back(ConvertPlainKeyToHerdKey(plain_key));
  }
  return trace;
}

void HerdWarmUp(HerdClient& cli, const std::vector<uint128>& trace) {
  std::vector<HerdResp> resps;
  unsigned int trace_idx = 0;
  for (unsigned int i = 0; i < kHerdWarmUpIter; i++) {
    auto key = trace[trace_idx];
    while (!cli.PostRequest(key, nullptr, HERD_VALUE_SIZE, false)) {
      cli.GetResponses(resps);
      for (const auto& resp : resps) {
        if (resp.Offloaded()) {
          offloaded.insert(std::make_pair(resp.CloverKey(), true));
        }
      }
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
  uint64_t hrd_rand_seed = global_id;

  unsigned long completed = 0;

  while (!stop_flag.load(std::memory_order_acquire)) {
    auto key = trace[trace_idx];
    bool update = (hrd_fastrand(&hrd_rand_seed) % 100U) < FLAGS_update_pct;
    while (!cli.PostRequest(key, nullptr, HERD_VALUE_SIZE, update)) {
      cli.GetResponses(resps);
      if (FLAGS_clover_threads > 0) {
        for (const auto& resp : resps) {
          if (resp.Offloaded()) {
            offloaded.insert(std::make_pair(resp.CloverKey(), true));
          }
        }
      }
    }

    HRD_MOD_ADD(trace_idx, trace.size());
    completed++;
  }

  unsigned long tput = completed / FLAGS_bench_secs;
  XLOGF(INFO, "HERD: {:8d} op/s.", tput);
  tput_prm.set_value(tput);
}

void CloverWorkCoro(coro_yield_t& yield, CloverCnThreadWrapper& cli,
                    const std::vector<mitsume_key>& trace,
                    unsigned int& trace_idx, int cid,
                    unsigned long& completed) {
  constexpr unsigned int kDummyBufSize = 4096;
  thread_local static char kCloverInvalidValue[] = "_clover_err";

  char dummy_buf[kDummyBufSize];
  unsigned int dummy_len;

  while (true) {
    mitsume_key key = trace[trace_idx];
    if (offloaded.find(key) != offloaded.end()) {
      int rc =
          cli.ReadKVPair(key, dummy_buf, &dummy_len, kDummyBufSize, cid, yield);
      completed++;
      if (rc == MITSUME_SUCCESS &&
          strncmp(dummy_buf, kCloverInvalidValue, kDummyBufSize) == 0) {
        rc = MITSUME_ERROR;
      }
      if (rc == MITSUME_ERROR) {
        offloaded.erase(key);
      }
    }

    HRD_MOD_ADD(trace_idx, trace.size());
  }
}

void CloverMainCoro(coro_yield_t& yield, CloverCnThreadWrapper& cli,
                    std::atomic_bool& stop_flag) {
  while (!stop_flag.load(std::memory_order_acquire)) {
    cli.YieldToAnotherCoro(kMasterCoroutineIdx, yield);
  }
}

void CloverMain(CloverComputeNodeWrapper& node, int tid,
                boost::barrier& barrier, std::atomic_bool& stop_flag,
                std::promise<unsigned long> tput_prm) {
  using clock = std::chrono::steady_clock;
  using std::placeholders::_1;
  CloverCnThreadWrapper cli(node, tid);

  auto herd_trace = GenerateTrace(tid);
  std::vector<mitsume_key> trace;
  for (auto herd_key : herd_trace) {
    trace.push_back(ConvertHerdKeyToCloverKey(
        herd_key, HerdClient::PickRemoteWorker(herd_key)));
  }

  unsigned int trace_idx = 0;
  unsigned long completed = 0;

  cli.RegisterCoroutine(coro_call_t(std::bind(CloverMainCoro, _1, std::ref(cli),
                                              std::ref(stop_flag))),
                        kMasterCoroutineIdx);
  for (int i = 1; i <= kCloverCoros; i++) {
    cli.RegisterCoroutine(
        coro_call_t(std::bind(CloverWorkCoro, _1, std::ref(cli),
                              std::cref(trace), std::ref(trace_idx), i,
                              std::ref(completed))),
        i);
  }

  XLOGF(INFO, "Clover consumer {:2d} is ready.", tid);
  barrier.wait();

  auto start = clock::now();
  cli.ExecuteMasterCoroutine();
  auto end = clock::now();

  double sec = std::chrono::duration<double>(end - start).count();
  unsigned long tput = std::lround(completed / sec);
  XLOGF(INFO, "Clover: {:8d} op/s", tput);
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
      FLAGS_clover_memcached_ip = reg_ip_env;
    }
  }
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();

  XLOGF(INFO, "Clover memcached: {}", FLAGS_clover_memcached_ip);
  XLOGF(INFO, "Using {} HERD threads and {} Clover consumer threads.",
        FLAGS_herd_threads, FLAGS_clover_threads);
  XLOGF(INFO, "Fraction of update operations: {}%", FLAGS_update_pct);
  XLOGF(INFO, "Value size: {} bytes.", HERD_VALUE_SIZE);
  XLOG_IF(WARN, MITSUME_CLT_CONSUMER_GC_THREAD_NUMS > 0,
          "Using Clover GC threads at client-side is unnecessary.");

  CloverComputeNodeWrapper clvr_node(FLAGS_clover_threads);
  if (FLAGS_clover_threads > 0) {
    clvr_node.Initialize();
    XLOG(INFO, "Sleep 3 secs to wait Clover metadata server completes setup.");
    std::this_thread::sleep_for(std::chrono::seconds(3));
  } else {
    XLOG(INFO, "Skipping Clover client initialization.");
  }

  boost::barrier barrier(FLAGS_herd_threads + FLAGS_clover_threads + 1);
  std::atomic_bool stop_flag = ATOMIC_VAR_INIT(false);

  std::vector<std::thread> threads;
  std::vector<std::future<unsigned long>> herd_tput_futs, clvr_tput_futs;

  for (unsigned int i = 0; i < FLAGS_herd_threads; i++) {
    std::promise<unsigned long> prm;
    herd_tput_futs.push_back(prm.get_future());
    threads.emplace_back(HerdMain, FLAGS_herd_mach_id * FLAGS_herd_threads + i,
                         std::ref(barrier), std::ref(stop_flag),
                         std::move(prm));
  }
  for (unsigned int i = 0; i < FLAGS_clover_threads; i++) {
    std::promise<unsigned long> prm;
    clvr_tput_futs.push_back(prm.get_future());
    threads.emplace_back(CloverMain, std::ref(clvr_node), i, std::ref(barrier),
                         std::ref(stop_flag), std::move(prm));
  }
  if (FLAGS_pin_threads) {
    XLOG(INFO, "Pin benchmark threads to specific CPU core");
    unsigned int core = 0;
    for (auto& t : threads) {
      SetAffinity(t, core++);
    }
  }
  threads.emplace_back(CountDownMain, std::ref(barrier), std::ref(stop_flag));

  unsigned long herd_tput = 0, clvr_tput = 0;
  for (auto& fut : herd_tput_futs) {
    herd_tput += fut.get();
  }
  for (auto& fut : clvr_tput_futs) {
    clvr_tput += fut.get();
  }
  XLOGF(INFO, "HERD: {} op/s, Clover: {} op/s", herd_tput, clvr_tput);
  for (auto& t : threads) {
    t.join();
  }
  return 0;
}
