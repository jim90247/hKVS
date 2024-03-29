#pragma once
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <moodycamel/concurrentqueue.h>

#include <memory>

#include "clover_wrapper/cn.h"

/// Number of clover worker threads
DECLARE_int32(clover_threads);
/// Number of worker coroutines in each Clover thread
DECLARE_int32(clover_coros);

using CloverRequestIdType = uint32_t;

enum class CloverRequestType { kInsert, kWrite, kInvalidate, kRead };

enum class CloverReplyOption { kAlways, kNever, kOnFailure };

struct CloverRequest {
  mitsume_key key;
  void *buf;
  int len;
  CloverRequestIdType id;
  CloverRequestType op;
  int from;
  CloverReplyOption reply_opt;
};

struct CloverResponse {
  mitsume_key key;
  CloverRequestIdType id;
  CloverRequestType op;
  int rc;
};

/// Multi-threaded request queue
using SharedRequestQueue = moodycamel::ConcurrentQueue<CloverRequest>;
using SharedRequestQueuePtr = std::shared_ptr<SharedRequestQueue>;
/// Multi-threaded response queue
using SharedResponseQueue = moodycamel::ConcurrentQueue<CloverResponse>;
using SharedResponseQueuePtr = std::shared_ptr<SharedResponseQueue>;

/**
 * @brief Main function of Clover thread.
 *
 * @param clover_node the clover compute node wrapper
 * @param req_queue the shared request queue
 * @param resp_queues the pointers to the response queues for each producer
 * @param clover_thread_id the id of this clover thread (starting from 0)
 */
void CloverThreadMain(CloverComputeNodeWrapper &clover_node,
                      SharedRequestQueue &req_queue,
                      const std::vector<SharedResponseQueuePtr> &resp_queues,
                      int clover_thread_id);
