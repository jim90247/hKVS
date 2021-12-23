#include "clover_worker.h"

#include <glog/logging.h>
#include <glog/raw_logging.h>

#include <cstring>
#include <vector>

DEFINE_int32(clover_threads, 4, "Number of clover worker threads");
DEFINE_int32(clover_coros, 1,
             "Number of worker coroutines in each Clover thread");

void CloverWorkerCoro(coro_yield_t &yield, CloverCnThreadWrapper &cn_thread,
                      SharedRequestQueue &req_queue,
                      const std::vector<SharedResponseQueuePtr> &resp_queues,
                      moodycamel::ConsumerToken &ctok, int coro) {
  // Special value indicating the corresponding key is invalid
  thread_local static char kCloverInvalidValue[] = "_clover_err";

  CloverRequest req;
  uint32_t dummy_len;
  while (true) {
    if (req_queue.try_dequeue(ctok, req)) {
      CloverResponse resp{
          req.key,  // key
          req.id,   // id
          req.op,   // op
          0         // rc
      };
      switch (req.op) {
        case CloverRequestType::kInsert:
          resp.rc = cn_thread.InsertKVPair(req.key, req.buf, req.len);
          break;
        case CloverRequestType::kInvalidate:
          req.buf = kCloverInvalidValue;
          req.len = sizeof(kCloverInvalidValue);
          [[fallthrough]];
        case CloverRequestType::kWrite:
          resp.rc = cn_thread.WriteKVPair(req.key, req.buf, req.len);
          break;
        case CloverRequestType::kRead:
          resp.rc = cn_thread.ReadKVPair(req.key, req.buf, &dummy_len, req.len,
                                         coro, yield);
          // treat invalidated key as error too
          if (resp.rc == MITSUME_SUCCESS &&
              strncmp(reinterpret_cast<char *>(req.buf), kCloverInvalidValue,
                      req.len) == 0) {
            resp.rc = MITSUME_ERROR;
          }
          break;
      }
      if (req.reply_opt == CloverReplyOption::kAlways ||
          (req.reply_opt == CloverReplyOption::kOnFailure &&
           resp.rc != MITSUME_SUCCESS)) {
        while (!resp_queues.at(req.from)->try_enqueue(resp)) {
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
