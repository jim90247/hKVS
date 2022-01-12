#include <gflags/gflags.h>
#include <glog/logging.h>

#include <chrono>
#include <queue>
#include <unordered_map>
#include <vector>

#include "util/lru_records.h"
#include "util/zipfian_generator.h"

DEFINE_int64(lru_size, 8192L, "Size of LRU record");
DEFINE_uint64(trace_size, 16UL << 20, "Length of trace");
DEFINE_double(zipfian_alpha, 0.99, "Zipfian parameter");
DEFINE_uint64(key_range, 8UL << 20, "Key range");
DEFINE_uint64(window_size, 64UL << 10, "Time window size");
DEFINE_uint64(window_min_count, 4,
              "Minimum amount of appearance to be placed in cache");
DEFINE_int32(seed, 42, "Random seed for trace generation");

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
  FLAGS_alsologtostderr = true;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  LruRecordsWithMinCount<int> cache(FLAGS_lru_size, FLAGS_window_size,
                                    FLAGS_window_min_count);
  // LruRecords<int> cache(FLAGS_lru_size);

  auto start = clock::now();
  auto trace = GenerateZipfianTrace(FLAGS_seed);
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

  LOG(INFO) << "Record=" << FLAGS_lru_size << "," << FLAGS_trace_size << ","
            << FLAGS_zipfian_alpha << "," << FLAGS_key_range << ","
            << FLAGS_window_size << "," << FLAGS_window_min_count << ","
            << FLAGS_seed << "," << static_cast<double>(hit) / FLAGS_trace_size
            << "," << evict << ","
            << FLAGS_trace_size /
                   std::chrono::duration<double>(end - start).count();

  hit = 0;
  for (auto x : trace) {
    if (x < FLAGS_lru_size) {
      hit++;
    }
  }
  LOG(INFO) << "Optimal static offload = "
            << static_cast<double>(hit) / FLAGS_trace_size;
  return 0;
}
