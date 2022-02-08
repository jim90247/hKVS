#include "req_submitter.h"

CloverRequestQueueHandler::CloverRequestQueueHandler(
    SharedRequestQueuePtr req_queue_ptr, unsigned int concurrent_reqs,
    int thread_id)
    : req_queue_ptr_(req_queue_ptr),
      ptok_(*req_queue_ptr),
      max_cncr_reqs_(concurrent_reqs),
      max_batch_(std::min(concurrent_reqs, kInternalMaxBatch)),
      thread_id_(thread_id) {
  read_buf_base_addr_ = new char[max_cncr_reqs_ * kReadBufLen];
  for (CloverRequestIdType id = 0; id < max_cncr_reqs_; id++) {
    FreeSlot slot;
    slot.addr = read_buf_base_addr_ + id * kReadBufLen;
    slot.id = id;
    slot.in_use = false;
    reqid_to_slot_.push_back(slot);
  }
  next_reqid_ = 0;
}

CloverRequestQueueHandler::~CloverRequestQueueHandler() {
  delete[] read_buf_base_addr_;
}

CloverReqSubmitError CloverRequestQueueHandler::TrySubmitRead(
    mitsume_key key, CloverRequestType op) {
  if (reqid_to_slot_[next_reqid_].in_use) {
    return kTooManyReqs;
  }
  return TrySubmit(key, op, reqid_to_slot_[next_reqid_].addr, kReadBufLen);
}

CloverReqSubmitError CloverRequestQueueHandler::TrySubmitWrite(
    mitsume_key key, CloverRequestType op, void* val, unsigned int len) {
  if (reqid_to_slot_[next_reqid_].in_use) {
    return kTooManyReqs;
  }
  return TrySubmit(key, op, val, len);
}

CloverReqSubmitError CloverRequestQueueHandler::TrySubmit(mitsume_key key,
                                                          CloverRequestType op,
                                                          void* val,
                                                          unsigned int len) {
  CloverRequest req;
  req.buf = val;
  req.from = thread_id_;
  req.id = next_reqid_;
  req.key = key;
  req.len = len;
  req.op = op;
  req.reply_opt = CloverReplyOption::kAlways;
  req_buf_.push_back(req);
  reqid_to_slot_[next_reqid_].in_use = true;

  if (req_buf_.size() >= max_batch_) {
    Flush();
  }

  if (++next_reqid_ == max_cncr_reqs_) {
    next_reqid_ = 0;
  }

  return kSuccess;
}

void CloverRequestQueueHandler::Flush() {
  auto ok = req_queue_ptr_->try_enqueue_bulk(ptok_, req_buf_.begin(),
                                             req_buf_.size());
  if (!ok) {
    throw std::runtime_error("try_enqueue failed");
  }
  req_buf_.clear();
}

void CloverRequestQueueHandler::ReclaimSlot(CloverRequestIdType req_id) {
  reqid_to_slot_[req_id].in_use = false;
}

