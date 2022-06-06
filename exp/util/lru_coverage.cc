#include <gflags/gflags.h>
#include <glog/logging.h>

#include <chrono>
#include <queue>
#include <vector>

#include "util/lru_records.h"
#include "util/zipfian_generator.h"

DEFINE_uint64(lru_size, 100'000UL, "Size of LRU record");
DEFINE_uint64(trace_size, 16UL << 20, "Length of trace");
DEFINE_double(zipfian_alpha, 0.99, "Zipfian parameter");
DEFINE_uint64(key_range, 8UL << 20, "Key range");
DEFINE_uint64(window_size, 800'000UL, "Time window size");
DEFINE_uint64(window_min_count, 4,
              "Minimum amount of appearance to be placed in cache");

/**
 * @brief Generates a key access trace based on Zipfian distribution. Key range:
 * [0, FLAGS_key_range).
 *
 * @param seed the random seed
 * @return a vector of integers in range [0, FLAGS_key_range) representing the
 * trace
 */
std::vector<int> GenerateZipfianTrace(int seed) {
  ZipfianGenerator gen(FLAGS_key_range, FLAGS_zipfian_alpha, seed);
  std::vector<int> trace(FLAGS_trace_size);
  for (size_t i = 0; i < FLAGS_trace_size; i++) {
    trace[i] = gen.GetNumber();
  }
  return trace;
}

int main(int argc, char** argv) {
  using clock = std::chrono::steady_clock;
  FLAGS_colorlogtostderr = true;
  FLAGS_logtostderr = true;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  LruRecordsWithMinCount<int64_t> cache(FLAGS_lru_size, FLAGS_window_size,
                                        FLAGS_window_min_count);
  // LruRecords<int64_t> cache(FLAGS_lru_size);

  {
    auto trace = GenerateZipfianTrace(123);
    // The warmup requests per HERD worker thread under following settings:
    // 3M requests per client, LRU sample rate = 1/256, 24 HERD worker threads,
    // 72 HERD client threads
    for (unsigned int i = 0; i < 35156; i++) {
      cache.Put(trace.at(i));
    }
  }

  auto start = clock::now();
  auto trace = GenerateZipfianTrace(42);
  auto end = clock::now();
  LOG(INFO) << "Generate zipfian trace of size " << FLAGS_trace_size
            << " takes " << std::chrono::duration<double>(end - start).count()
            << " seconds";
  LOG(INFO) << "Starting benchmark";
  start = clock::now();
  unsigned long hit = 0, evict = 0;
  for (size_t i = 1; i <= FLAGS_trace_size; i++) {
    int key = trace.at(i - 1);
    if (cache.Contain(key)) {
      hit++;
    }
    if (cache.Put(key).has_value()) {
      evict++;
    }
  }
  end = clock::now();
  LOG(INFO) << "Performance = "
            << FLAGS_trace_size /
                   std::chrono::duration<double>(end - start).count()
            << " op/s";
  LOG(INFO) << "Hit rate = " << static_cast<double>(hit) / FLAGS_trace_size
            << ", Eviction = " << evict;

  hit = 0;
  for (auto x : trace) {
    if (x < FLAGS_lru_size) {
      hit++;
    }
  }
  LOG(INFO) << "Optimal static offload = "
            << static_cast<double>(hit) / FLAGS_trace_size;
  LOG(INFO) << "Estimated memory usage = " << cache.EstimatedMemoryUsage()
            << " bytes";
  return 0;
}
