#include "op_counter.h"

#include <cassert>
#include <iostream>

#define unlikely(x) __builtin_expect(!!(x), 0)

namespace opctr {
// The mapping from operation to their names
const std::vector<std::string> op_names = {
    "userspace_one_read",
    "userspace_one_read_sge (one per sge)",
    "userspace_one_send",
    "userspace_one_write",
    "userspace_one_write_inline",
    "userspace_one_write_sge (one per sge)",
    "userspace_one_cs"};

thread_local int thread_id = -1;
std::vector<OperationStats> thread_stats;
std::mutex thread_stats_mutex;

void RecordOperation(Operation op) {
  assert(op != Operation::kNumOperations);
  if (unlikely(thread_id == -1)) {
    std::lock_guard<std::mutex> lock(thread_stats_mutex);
    thread_id = thread_stats.size();
    thread_stats.push_back(OperationStats());
  }
  thread_stats.at(thread_id).count[op]++;
}

void PrintTotalOperations() {
  std::cerr << "Mitsume operation statistics:" << std::endl;
  unsigned long op_total[Operation::kNumOperations] = {0UL};
  for (int op = 0; op < Operation::kNumOperations; op++) {
    for (int t = 0; t < thread_stats.size(); t++) {
      op_total[op] += thread_stats[t].count[op];
    }
    std::cerr << op_names.at(op) << ": " << op_total[op] << std::endl;
  }
  std::cerr << "Total RDMA read: "
            << op_total[Operation::kUserspaceOneRead] +
                   op_total[Operation::kUserspaceOneReadSge]
            << std::endl;
  std::cerr << "Total RDMA write: "
            << op_total[Operation::kUserspaceOneWrite] +
                   op_total[Operation::kUserspaceOneWriteInline] +
                   op_total[Operation::kUserspaceOneWriteSge]
            << std::endl;
}
};  // namespace opctr
