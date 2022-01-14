#include <gflags/gflags.h>
#include <glog/logging.h>

#include <chrono>
#include <queue>
#include <unordered_map>
#include <vector>

#include "csv.hpp"
#include "util/lru_records.h"
#include "util/twitter_trace_reader.h"
#include "util/zipfian_generator.h"

/// LRU params
DEFINE_int64(lru_size, 8192L, "Size of LRU record");
DEFINE_uint64(window_size, 64UL << 10, "Time window size");
DEFINE_uint64(window_min_count, 4,
              "Minimum amount of appearance to be placed in cache");
/// Common trace params
DEFINE_uint64(trace_size, 16UL << 20, "Length of trace to read/generate");
/// Twitter cache trace params
DEFINE_string(twttr_trace, "", "Path to Twitter cache trace CSV");
/// Generator params
DEFINE_double(zipfian_alpha, 0.99, "Zipfian parameter");
DEFINE_uint64(key_range, 8UL << 20, "Key range");
DEFINE_int32(seed, 42, "Random seed for trace generation");

std::vector<TraceKey> BuildTrace() {
  std::vector<TraceKey> trace(FLAGS_trace_size);
  std::unique_ptr<TraceProvider> trace_provider;
  if (FLAGS_twttr_trace.empty()) {
    trace_provider = std::make_unique<ZipfianGenerator>(
        FLAGS_key_range, FLAGS_zipfian_alpha, FLAGS_seed);
  } else {
    TwttrTraceFilterOption filter_opt;
    filter_opt.ops = {"get", "gets"};
    trace_provider = std::make_unique<TwttrTraceReader>(
        FLAGS_twttr_trace, filter_opt, FLAGS_trace_size);
  }
  for (size_t i = 0; i < FLAGS_trace_size; i++) {
    trace[i] = trace_provider->GetNumber();
  }
  return trace;
}

int main(int argc, char** argv) {
  using clock = std::chrono::steady_clock;
  FLAGS_colorlogtostderr = true;
  FLAGS_alsologtostderr = true;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  LruRecordsWithMinCount<TraceKey> cache(FLAGS_lru_size, FLAGS_window_size,
                                         FLAGS_window_min_count);
  // LruRecords<TraceKey> cache(FLAGS_lru_size);

  auto trace = BuildTrace();

  LOG(INFO) << "Starting benchmark";
  auto start = clock::now();
  size_t hit = 0, evict = 0;
  for (auto key : trace) {
    if (cache.Contain(key)) {
      hit++;
    }
    if (cache.Put(key).has_value()) {
      evict++;
    }
  }
  auto end = clock::now();

  double ops =
      FLAGS_trace_size / std::chrono::duration<double>(end - start).count();
  double hit_rate = static_cast<double>(hit) / FLAGS_trace_size;
  LOG(INFO) << "Performance = " << ops << " op/s";
  LOG(INFO) << "Hit rate = " << hit_rate << ", Eviction = " << evict;

  std::stringstream ss;
  auto csv_writer = csv::make_csv_writer(ss);
  if (FLAGS_twttr_trace.empty()) {
    hit = 0;
    for (auto x : trace) {
      if (x < FLAGS_lru_size) {
        hit++;
      }
    }
    LOG(INFO) << "Optimal static offload = "
              << static_cast<double>(hit) / FLAGS_trace_size;

    csv_writer << std::make_tuple(FLAGS_lru_size, FLAGS_trace_size,
                                  FLAGS_zipfian_alpha, FLAGS_key_range,
                                  FLAGS_window_size, FLAGS_window_min_count,
                                  FLAGS_seed, hit_rate, evict, ops);
  } else {
    csv_writer << std::make_tuple(FLAGS_lru_size, FLAGS_trace_size,
                                  FLAGS_twttr_trace, FLAGS_window_size,
                                  FLAGS_window_min_count, hit_rate, evict, ops);
  }
  LOG(INFO) << "Record=" << ss.str();
  return 0;
}
