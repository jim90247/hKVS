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
DEFINE_int32(global_cid, 0, "Global client id");
DEFINE_bool(update, false, "Test update operation instead of read");
DEFINE_uint32(bench_secs, 40, "Seconds to run benchmark");

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

void BenchmarkOneOperation(HerdClient& cli, const std::vector<uint128>& trace,
                           bool update, std::atomic_bool& stop_flag) {
  auto cycle_per_us = MeasureClockFreq();

  unsigned int idx = 0;
  std::vector<HerdResp> resps;
  std::vector<double> latencies;

  while (!stop_flag.load(std::memory_order_acquire)) {
    auto start = Rdtscp();
    auto& key = trace[idx];
    cli.PostRequest(key, nullptr, HERD_VALUE_SIZE, update,
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
        update ? "update" : "read", avg, median, tail, max);

  double tput = 1e6 / avg;
  XLOGF(INFO, "{:.2f} {}/s", tput, update ? "update" : "read");
}

void BenchmarkMain(HerdClient& cli, boost::barrier& barrier,
                   std::atomic_bool& stop_flag) {
  auto trace = GenerateTrace();

  auto cycle_per_us = MeasureClockFreq();
  XLOGF(INFO, "Cycles per micro-second: {}.", cycle_per_us);

  WarmUp(cli, trace);
  XLOG(INFO, "warm-up completed.");
  barrier.wait();

  BenchmarkOneOperation(cli, trace, FLAGS_update, stop_flag);
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

  XLOGF(INFO, "Client id: {}", FLAGS_global_cid);

  {
    auto memcached_ip = std::getenv("HRD_REGISTRY_IP");
    XCHECK_NE(memcached_ip, nullptr);
    XLOGF(INFO, "Memcached server: {}", memcached_ip);
  }
  XLOGF(INFO, "Value size: {}", HERD_VALUE_SIZE);

  HerdClient cli(FLAGS_global_cid, FLAGS_server_ports, FLAGS_client_ports,
                 FLAGS_base_port_index);
  cli.ConnectToServer();

  boost::barrier barrier(2);
  std::atomic_bool stop_flag = ATOMIC_VAR_INIT(false);
  std::thread countdown_thread(CountDownMain, std::ref(barrier),
                               std::ref(stop_flag));
  std::thread bench_thread(BenchmarkMain, std::ref(cli), std::ref(barrier),
                           std::ref(stop_flag));

  SetAffinity(bench_thread, 0);
  XLOG(INFO, "Pin benchmark thread to core 0.");

  countdown_thread.join();
  bench_thread.join();

  return 0;
}
