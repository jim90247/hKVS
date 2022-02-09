#pragma once

#include <forward_list>
#include <limits>
#include <map>
#include <queue>
#include <vector>

#include "clover_worker.h"

enum CloverReqSubmitError {
  kSuccess,
  kTooManyReqs,
};

struct FreeSlot {
  CloverRequestIdType id;
  char* addr;
  bool in_use;
};

// one for each request queue
class CloverRequestQueueHandler {
 public:
  static constexpr unsigned int kInternalMaxBatch = 16;
  static constexpr unsigned int kReadBufLen = 4096;

  CloverRequestQueueHandler(SharedRequestQueuePtr req_queue_ptr,
                            unsigned int concurrent_reqs, int thread_id);
  CloverRequestQueueHandler(CloverRequestQueueHandler&& other);
  ~CloverRequestQueueHandler();
  CloverReqSubmitError TrySubmitRead(mitsume_key key);
  CloverReqSubmitError TrySubmitWrite(mitsume_key key, CloverRequestType op,
                                      void* val, unsigned int len);
  void Flush();
  /**
   * @brief Marks the slots corresponding to the repesentative request id as
   * unused.
   *
   * @param rep_id representative request id
   */
  void ReclaimSlot(CloverRequestIdType rep_id);
  /**
   * @brief Gets the amount of buffered requests.
   */
  inline unsigned int GetBuffered() const noexcept { return req_buf_.size(); }
  /**
   * @brief Gets the amount of submitted but not completed requests.
   */
  inline unsigned int GetPending() const noexcept { return pending_; }

 private:
  SharedRequestQueuePtr req_queue_ptr_;
  moodycamel::ProducerToken ptok_;
  const unsigned int max_cncr_reqs_;
  const unsigned int max_batch_;
  const int thread_id_;

  char* read_buf_base_addr_;
  std::vector<FreeSlot> reqid_to_slot_;
  std::vector<CloverRequest> req_buf_;
  unsigned int next_reqid_;
  std::vector<std::vector<CloverRequestIdType>> reclaim_lists_;
  unsigned int pending_;

  CloverReqSubmitError TrySubmit(mitsume_key key, CloverRequestType op,
                                 void* val, unsigned int len);
};

class CloverRequestSubmitter {
 public:
  CloverRequestSubmitter(unsigned int max_concurrent_reqs,
                         const std::vector<SharedRequestQueuePtr>& req_queues,
                         SharedResponseQueuePtr resp_queue, int thread_id);
  CloverReqSubmitError TrySubmitRead(mitsume_key key);
  CloverReqSubmitError TrySubmitWrite(mitsume_key key, CloverRequestType op,
                                      void* val, unsigned int len);
  void Flush();
  std::vector<CloverResponse> GetResponses();
  /**
   * @brief Gets the amount of submitted (flushed) but not completed requests.
   */
  unsigned int GetPending();

 private:
  const unsigned int max_concurrent_reqs_;

  std::vector<SharedRequestQueuePtr> req_queue_ptrs_;
  std::vector<CloverRequestQueueHandler> handlers_;
  SharedResponseQueuePtr resp_queue_ptr_;

  unsigned int PickRequestQueue(mitsume_key key);
};

std::vector<CloverResponse> BlockingSubmitWrite(
    CloverRequestSubmitter& submitter, mitsume_key key, CloverRequestType op,
    void* val, unsigned int len);

std::vector<CloverResponse> BlockUntilAllComplete(
    CloverRequestSubmitter& submitter);
