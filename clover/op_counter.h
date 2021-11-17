#include <mutex>
#include <vector>

namespace opctr {
/**
 * @brief Operations to collect statistics.
 */
enum Operation {
  kUserspaceOneRead,
  // each sge element is counted separately
  kUserspaceOneReadSge,
  kUserspaceOneSend,
  kUserspaceOneWrite,
  kUserspaceOneWriteInline,
  // each sge element is counted separately
  kUserspaceOneWriteSge,
  kUserspaceOneCs,
  // the number of operations to collect statistics
  kNumOperations,
};

struct OperationStats {
  unsigned long count[Operation::kNumOperations] = {0UL};
};

extern thread_local int thread_id;

/**
 * @brief Increments the counter for the RDMA operation. Will initialize the
 * operation counter of current thread if it is not initialized yet.
 *
 * @param op the RDMA operation
 */
void RecordOperation(Operation op);

/**
 * @brief Print the total operation counts of each RDMA operation.
 */
void PrintTotalOperations();

};  // namespace opctr
