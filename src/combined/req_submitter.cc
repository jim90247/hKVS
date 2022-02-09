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
  reclaim_lists_.resize(max_cncr_reqs_);
  next_reqid_ = 0;
  pending_ = 0;
}

CloverRequestQueueHandler::CloverRequestQueueHandler(
    CloverRequestQueueHandler&& other)
    : req_queue_ptr_(std::move(other.req_queue_ptr_)),
      ptok_(std::move(other.ptok_)),
      max_cncr_reqs_(other.max_cncr_reqs_),
      max_batch_(other.max_batch_),
      thread_id_(other.thread_id_),
      read_buf_base_addr_(std::exchange(other.read_buf_base_addr_, nullptr)),
      reqid_to_slot_(std::move(other.reqid_to_slot_)),
      req_buf_(std::move(other.req_buf_)),
      next_reqid_(other.next_reqid_),
      reclaim_lists_(std::move(other.reclaim_lists_)),
      pending_(other.pending_) {}

CloverRequestQueueHandler::~CloverRequestQueueHandler() {
  if (read_buf_base_addr_ != nullptr) {
    delete[] read_buf_base_addr_;
  }
}

CloverReqSubmitError CloverRequestQueueHandler::TrySubmitRead(mitsume_key key) {
  if (reqid_to_slot_[next_reqid_].in_use) {
    return kTooManyReqs;
  }
  return TrySubmit(key, CloverRequestType::kRead,
                   reqid_to_slot_[next_reqid_].addr, kReadBufLen);
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
  req.reply_opt = CloverReplyOption::kOnFailure;
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
  if (req_buf_.empty()) {
    return;
  }
  req_buf_.back().reply_opt = CloverReplyOption::kAlways;

  auto ok = req_queue_ptr_->try_enqueue_bulk(ptok_, req_buf_.begin(),
                                             req_buf_.size());
  if (!ok) {
    throw std::runtime_error("try_enqueue failed");
  }

  auto& reclaim_list = reclaim_lists_[req_buf_.back().id];
  for (auto& req : req_buf_) {
    reclaim_list.push_back(req.id);
  }

  pending_ += req_buf_.size();
  req_buf_.clear();
}

void CloverRequestQueueHandler::ReclaimSlot(CloverRequestIdType rep_id) {
  for (auto rec_id : reclaim_lists_[rep_id]) {
    reqid_to_slot_[rec_id].in_use = false;
  }
  pending_ -= reclaim_lists_[rep_id].size();
  reclaim_lists_[rep_id].clear();
}

CloverRequestSubmitter::CloverRequestSubmitter(
    unsigned int max_concurrent_reqs,
    std::vector<SharedRequestQueuePtr> req_queues,
    SharedResponseQueuePtr resp_queue, int thread_id)
    : max_concurrent_reqs_(max_concurrent_reqs),
      req_queue_ptrs_(req_queues),
      resp_queue_ptr_(resp_queue) {
  for (unsigned int i = 0; i < req_queue_ptrs_.size(); i++) {
    handlers_.emplace_back(req_queue_ptrs_[i], max_concurrent_reqs_, thread_id);
  }
}

unsigned int CloverRequestSubmitter::PickRequestQueue(mitsume_key key) {
  return key % req_queue_ptrs_.size();
}

CloverReqSubmitError CloverRequestSubmitter::TrySubmitRead(mitsume_key key) {
  auto target_queue_id = PickRequestQueue(key);
  return handlers_.at(target_queue_id).TrySubmitRead(key);
}

CloverReqSubmitError CloverRequestSubmitter::TrySubmitWrite(
    mitsume_key key, CloverRequestType op, void* val, unsigned int len) {
  auto target_queue_id = PickRequestQueue(key);
  return handlers_.at(target_queue_id).TrySubmitWrite(key, op, val, len);
}

void CloverRequestSubmitter::Flush() {
  for (auto& handler : handlers_) {
    handler.Flush();
  }
}

std::vector<CloverResponse> CloverRequestSubmitter::GetResponses() {
  constexpr unsigned int kBatch = 8;
  std::vector<CloverResponse> resps(kBatch);
  auto num_resps = resp_queue_ptr_->try_dequeue_bulk(resps.begin(), kBatch);
  resps.resize(num_resps);
  for (auto& resp : resps) {
    auto target_queue_id = PickRequestQueue(resp.key);
    handlers_.at(target_queue_id).ReclaimSlot(resp.id);
  }
  return resps;
}

unsigned int CloverRequestSubmitter::GetPending() {
  unsigned int total = 0;
  for (auto& handler : handlers_) {
    total += handler.GetPending();
  }
  return total;
}
