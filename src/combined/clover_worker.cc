#include "clover_worker.h"

#include <glog/raw_logging.h>

#include <vector>

DEFINE_int32(clover_threads, 4, "Number of clover worker threads");
DEFINE_int32(clover_coros, 1,
             "Number of worker coroutines in each Clover thread");
DEFINE_bool(clover_blocking, false,
            "Wait for Clover request complete before continuing");

void CloverWorkerCoro(coro_yield_t &yield, CloverCnThreadWrapper &cn_thread,
                      SharedRequestQueue &req_queue,
                      const std::vector<SharedResponseQueuePtr> &resp_queues,
                      moodycamel::ConsumerToken &ctok, int coro) {
  CloverRequest req;
  while (true) {
    if (req_queue.try_dequeue(ctok, req)) {
      int rc = MITSUME_SUCCESS;
      switch (req.type) {
        case CloverRequestType::kInsert:
          rc = cn_thread.InsertKVPair(req.key, req.buf, req.len);
          break;
        case CloverRequestType::kWrite:
          rc = cn_thread.WriteKVPair(req.key, req.buf, req.len);
          break;
      }
      if (rc != MITSUME_SUCCESS) {
        RAW_LOG(FATAL, "Operation %d failed: key=%lu, rc=%d",
                static_cast<int>(req.type), req.key, rc);
      }
      if (req.need_reply) {
        while (!resp_queues.at(req.from)->try_enqueue(CloverResponse{req.id})) {
          cn_thread.YieldToAnotherCoro(coro, yield);
        }
      }
    }
    cn_thread.YieldToAnotherCoro(coro, yield);
  }
}

void CloverMainCoro(coro_yield_t &yield, CloverCnThreadWrapper &cn_thread) {
  while (true) {
    cn_thread.YieldToAnotherCoro(kMasterCoroutineIdx, yield);
  }
}

void CloverThreadMain(CloverComputeNodeWrapper &clover_node,
                      SharedRequestQueue &req_queue,
                      const std::vector<SharedResponseQueuePtr> &resp_queues,
                      int clover_thread_id) {
  using std::placeholders::_1;
  CloverCnThreadWrapper clover_thread(std::ref(clover_node), clover_thread_id);
  // coroutines in same thread share the same consumer token
  moodycamel::ConsumerToken ctok(req_queue);

  // dummy main coroutine
  clover_thread.RegisterCoroutine(
      coro_call_t(std::bind(CloverMainCoro, _1, std::ref(clover_thread))),
      kMasterCoroutineIdx);

  for (int coro = 1; coro <= FLAGS_clover_coros; coro++) {
    clover_thread.RegisterCoroutine(
        coro_call_t(std::bind(CloverWorkerCoro, _1, std::ref(clover_thread),
                              std::ref(req_queue), std::cref(resp_queues),
                              std::ref(ctok), coro)),
        coro);
  }

  clover_thread.ExecuteMasterCoroutine();
}
