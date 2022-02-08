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
  ~CloverRequestQueueHandler();
  CloverReqSubmitError TrySubmitRead(mitsume_key key, CloverRequestType op);
  CloverReqSubmitError TrySubmitWrite(mitsume_key key, CloverRequestType op,
                                      void* val, unsigned int len);
  void Flush();
  void ReclaimSlot(CloverRequestIdType req_id);

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

  CloverReqSubmitError TrySubmit(mitsume_key key, CloverRequestType op,
                                 void* val, unsigned int len);
};

