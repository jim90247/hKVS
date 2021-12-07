#pragma once
#include <gflags/gflags.h>

#include <thread>

#include "clover/mitsume.h"

DECLARE_int32(clover_machine_id);
DECLARE_int32(clover_ib_dev);
DECLARE_int32(clover_ib_port);
DECLARE_int32(clover_cn);
DECLARE_int32(clover_dn);
DECLARE_int32(clover_loopback);
DECLARE_string(clover_memcached_ip);

extern int MITSUME_CLT_NUM;
extern int MITSUME_MEM_NUM;

constexpr int kMasterCoroutineIdx = MITSUME_CLT_TEMP_COROUTINE_ID;
static_assert(kMasterCoroutineIdx < MITSUME_CLT_COROUTINE_NUMBER);

// Forward declaration
class CloverCnThreadWrapper;

class CloverComputeNodeWrapper {
  friend CloverCnThreadWrapper;

 public:
  /**
   * @brief Constructs a new Clover compute node wrapper. Note that this
   * constructor does not initialize the connection to metadata server.
   *
   * @param workers the number of worker threads
   * @see Initialize
   */
  CloverComputeNodeWrapper(int workers);
  CloverComputeNodeWrapper(const CloverComputeNodeWrapper&) = delete;
  CloverComputeNodeWrapper& operator=(const CloverComputeNodeWrapper&) = delete;
  ~CloverComputeNodeWrapper();
  /**
   * @brief Initializes the compute node. Must be called before any operations.
   *
   * @return MITSUME_SUCCESS on success
   */
  int Initialize();

 private:
  const int global_thread_id_;
  const int local_thread_id_;
  /// Infiniband device index
  const int ib_dev_idx_;
  /// Infiniband port index
  const int ib_port_index_;
  const int num_loopback_;
  /// Clover machine id
  const int machine_id_;
  /// Number of metadata servers
  const int num_ms_;
  /// Number of compute nodes
  const int num_cn_;
  /// Number of data nodes
  const int num_dn_;
  /// Number of worker threads
  const int workers_;

  mitsume_ctx_clt* clover_ctx_;
  ib_inf* ib_inf_;

  int SetupPostRecv();  // copied from Clover source
  int GetShortcut();    // copied from Clover source
};

class CloverCnThreadWrapper {
 public:
  /**
   * @brief Constructs a new Clover compute node thread wrapper from a compute
   * node context.
   *
   * @param cn the compute node context
   * @param thread_id the worker thread id
   */
  CloverCnThreadWrapper(CloverComputeNodeWrapper& cn, int thread_id);
  CloverCnThreadWrapper(const CloverCnThreadWrapper&) = delete;
  CloverCnThreadWrapper& operator=(const CloverCnThreadWrapper&) = delete;
  /**
   * @brief Inserts a key-value pair into Clover.
   *
   * @param key the key
   * @param val the pointer to the value buffer
   * @param len the value size
   * @return MITSUME_SUCCESS on success
   * @note This function should not be executed concurrently
   */
  int InsertKVPair(mitsume_key key, const void* val, size_t len);
  /**
   * @brief Reads a key-value pair from Clover.
   *
   * @param[in] key the key
   * @param[out] val the pointer to the value buffer
   * @param[out] len the pointer to the value size buffer
   * @param[in] maxlen the value buffer size
   * @return MITSUME_SUCCESS on success
   * @note This function does not yield to other coroutines during execution.
   * @note This function uses the coroutine 0's resource (e.g. buffer).
   */
  int ReadKVPair(mitsume_key key, void* val, uint32_t* len, size_t maxlen);
  /**
   * @brief Reads a key-value pair from Clover with a specific coroutine.
   *
   * @param[in] key the key
   * @param[out] val the pointer to the value buffer
   * @param[out] len the pointer to the value size buffer
   * @param[in] maxlen the value buffer size
   * @param[in] coro the coroutine id
   * @param[in, out] yield the coroutine yield object
   * @return MITSUME_SUCCESS on success
   */
  int ReadKVPair(mitsume_key key, void* val, uint32_t* len, size_t maxlen,
                 int coro, coro_yield_t& yield);
  /**
   * @brief Updates the value of an existing key in Clover.
   *
   * @param key the key
   * @param val the pointer to the value buffer
   * @param len the value size
   * @return MITSUME_SUCCESS on success
   */
  int WriteKVPair(mitsume_key key, const void* val, size_t len);
  /**
   * @brief Yields to another active coroutine.
   * 
   * @param coro current coroutine id
   * @param yield the coroutine yield object
   */
  void YieldToAnotherCoro(int coro, coro_yield_t& yield);
  /**
   * @brief Registers the coroutine into scheduling queue.
   * 
   * @param coro the coroutine function with no argument
   * @param id the index of coroutine
   */
  void RegisterCoroutine(coro_call_t&& coro, int id);
  /**
   * @brief Launches the master coroutine (kMasterCoroutineIdx).
   */
  void ExecuteMasterCoroutine();

 private:
  const int thread_id_;
  CloverComputeNodeWrapper& node_;
  mitsume_consumer_metadata* const metadata_;
  // Follow the usage in mitsume_benchmark_ycsb, where only the first 4K bytes
  // are used. Buffer is allocated at mitsume_local_thread_setup()
  // (mitsume_util.cc line 209/215).
  const static size_t kBufSize = 4096;
  // Buffer for reading, pre-allocated during context initialization
  char* rbuf_[MITSUME_CLT_COROUTINE_NUMBER];
  // Buffer for writing
  char* wbuf_[MITSUME_CLT_COROUTINE_NUMBER];
};
