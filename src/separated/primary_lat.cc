#include <folly/logging/xlog.h>
#include <gflags/gflags.h>

#include <atomic>
#include <boost/thread/barrier.hpp>
#include <vector>

#include "herd_client.h"
#include "timing.h"
#include "util/affinity.h"
#include "util/zipfian_generator.h"

DEFINE_int32(server_ports, 1, "Number of server IB ports");
DEFINE_int32(client_ports, 1, "Number of client IB ports");
DEFINE_int32(base_port_index, 0, "Base IB port index");
DEFINE_int32(gcid_offset, 0, "Global client id offset");
DEFINE_bool(update, false, "Test update operation instead of read");
DEFINE_uint32(bench_secs, 40, "Seconds to run benchmark");
DEFINE_uint32(threads, 1, "Number of threads to run benchmark");
DEFINE_uint32(pin_offset, 0, "Offset of pinned CPU index");

std::atomic_uint64_t total_tput = ATOMIC_VAR_INIT(0);

std::vector<uint128> GenerateTrace() {
  constexpr size_t kTraceLength = 2'000'000;
  ZipfianGenerator gen(HERD_NUM_KEYS, 0.99);
  std::vector<uint128> trace;

  for (size_t i = 0; i < kTraceLength; i++) {
    int plain_key = gen.GetNumber();
    trace.emplace_back(ConvertPlainKeyToHerdKey(plain_key));
  }
  return trace;
}

void WarmUp(HerdClient& cli, const std::vector<uint128>& trace) {
  constexpr unsigned int kWarmUpIters = 1'000'000;

  std::vector<HerdResp> resps;
  for (unsigned int i = 0; i < kWarmUpIters; i++) {
    auto key = trace[i % trace.size()];
    while (!cli.PostRequest(key, nullptr, 0, false, key.second % NUM_WORKERS)) {
      cli.GetResponses(resps);
    }
  }
}

// Computes average, median, 99% percentile and max
std::tuple<double, double, double, double> CalculateStatistics(
    std::vector<double>& latencies) {
  std::sort(latencies.begin(), latencies.end());
  auto count = latencies.size();
  double avg = std::accumulate(latencies.begin(), latencies.end(), 0.0) / count;
  return std::make_tuple(avg, latencies[count / 2], latencies[count * 99 / 100],
                         latencies.back());
}

void BenchmarkMain(unsigned int gcid, boost::barrier& barrier,
                   std::atomic_bool& stop_flag) {
  auto trace = GenerateTrace();
  auto cycle_per_us = MeasureClockFreq();

  HerdClient cli(gcid, FLAGS_server_ports, FLAGS_client_ports,
                 FLAGS_base_port_index);
  cli.ConnectToServer();

  WarmUp(cli, trace);
  XLOG(INFO, "warm-up completed.");
  barrier.wait();

  unsigned int idx = 0;
  std::vector<HerdResp> resps;
  std::vector<double> latencies;

  while (!stop_flag.load(std::memory_order_acquire)) {
    auto start = Rdtscp();
    auto& key = trace[idx];
    cli.PostRequest(key, nullptr, HERD_VALUE_SIZE, FLAGS_update,
                    key.second % NUM_WORKERS);
    cli.GetResponses(resps);
    auto end = Rdtscp();

    double us = static_cast<double>(end - start) / cycle_per_us;
    latencies.push_back(us);
    HRD_MOD_ADD(idx, trace.size());
  }

  auto [avg, median, tail, max] = CalculateStatistics(latencies);
  XLOGF(INFO,
        "{} latency: avg={:.2f}, median={:.2f}, 99%={:.2f}, max={:.2f} (us).",
        FLAGS_update ? "update" : "read", avg, median, tail, max);

  unsigned long tput = 1e6 / avg;
  total_tput.fetch_add(tput);
}

void CountDownMain(boost::barrier& barrier, std::atomic_bool& stop_flag) {
  barrier.wait();
  XLOGF(INFO, "Start benchmarking for {} seconds.", FLAGS_bench_secs);
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_bench_secs));
  stop_flag.store(true, std::memory_order_release);
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();

  {
    auto memcached_ip = std::getenv("HRD_REGISTRY_IP");
    XCHECK_NE(memcached_ip, nullptr);
    XLOGF(INFO, "Memcached server: {}", memcached_ip);
  }
  XLOGF(INFO, "Value size: {}", HERD_VALUE_SIZE);

  auto cycle_per_us = MeasureClockFreq();
  XLOGF(INFO, "Cycles per micro-second: {}.", cycle_per_us);

  boost::barrier barrier(FLAGS_threads + 1);
  std::atomic_bool stop_flag = ATOMIC_VAR_INIT(false);

  XLOG(INFO, "Pin benchmark threads to specific core.");
  std::vector<std::thread> bench_threads;
  for (unsigned int i = 0; i < FLAGS_threads; i++) {
    bench_threads.emplace_back(BenchmarkMain, FLAGS_gcid_offset + i,
                               std::ref(barrier), std::ref(stop_flag));
    SetAffinity(bench_threads[i], FLAGS_pin_offset + i);
  }

  std::thread countdown_thread(CountDownMain, std::ref(barrier),
                               std::ref(stop_flag));

  countdown_thread.join();
  for (auto& t : bench_threads) {
    t.join();
  }

  XLOGF(INFO, "{} {}/s", total_tput.load(), FLAGS_update ? "update" : "read");

  return 0;
}
