#include "herd_client.h"

#include <fmt/core.h>
#include <folly/logging/xlog.h>

#include <cstdlib>
#include <numeric>

HerdResp::HerdResp(uint128 key, unsigned int rem, bool update)
    : key_(key), buf_(nullptr), rem_(rem), offloaded_(false), update_(update) {}

uint128 HerdResp::Key() const { return key_; }

mitsume_key HerdResp::CloverKey() const {
  return ConvertHerdKeyToCloverKey(key_, rem_);
}

const volatile uint8_t* HerdResp::ValuePtr() const { return buf_; }

bool HerdResp::Offloaded() const { return offloaded_; }

HerdClient::HerdClient(int gcid, int server_ports, int client_ports,
                       int base_port_index)
    : gcid_(gcid),
      num_server_ports_(server_ports),
      num_client_ports_(client_ports),
      req_buf_entries_(HERD_VALUE_SIZE > kInlineCutOff ? WINDOW_SIZE : 1U),
      req_buf_idx_(0),
      pending_req_(0),
      pending_unsig_(0),
      window_slots_(NUM_WORKERS, 0) {
  int port_index = base_port_index + gcid_ % num_client_ports_;
  int dgram_buf_size = std::max(kDgramEntrySize * WINDOW_SIZE, 4096);

  cb_ = hrd_ctrl_blk_init(gcid_,           // local hid
                          port_index,      // client ib port index
                          -1,              // numa node id
                          1,               // # of conn qps
                          1,               // is UC
                          nullptr,         // preallocated conn buf
                          4096,            // conn buf size
                          -1,              // conn buf shm key
                          1,               // # of dgram qps
                          dgram_buf_size,  // dgram buf size
                          -1               // dgram buf shm key
  );

  req_buf_ =
      static_cast<mica_op*>(memalign(4096, sizeof(mica_op) * req_buf_entries_));
  XCHECK_NE(req_buf_, nullptr);
  req_mr_ =
      ibv_reg_mr(cb_->pd, req_buf_, sizeof(mica_op) * req_buf_entries_, 0);
  XCHECK_NE(req_mr_, nullptr);

  for (int i = 0; i < WINDOW_SIZE; i++) {
    recv_sges_[i].length = kDgramEntrySize;
    recv_sges_[i].lkey = cb_->dgram_buf_mr->lkey;

    recv_wrs_[i].num_sge = 1;
  }

  std::vector<unsigned int> recv_wr_ids(WINDOW_SIZE);
  std::iota(recv_wr_ids.begin(), recv_wr_ids.end(), 0);
  PostRecvWrs(recv_wr_ids);
}

void HerdClient::ConnectToServer() {
  auto conn_qp_name = fmt::format("client-conn-{}", gcid_);
  auto dgram_qp_name = fmt::format("client-dgram-{}", gcid_);
  hrd_publish_conn_qp(cb_, 0, conn_qp_name.c_str());
  hrd_publish_dgram_qp(cb_, 0, dgram_qp_name.c_str());
  XLOGF(INFO, "Client {} published conn and dgram QPs.", gcid_);

  int server_port_index = gcid_ % num_server_ports_;
  auto master_qp_name = fmt::format("master-{}-{}", server_port_index, gcid_);
  master_qp_ = nullptr;
  while (master_qp_ == nullptr) {
    master_qp_ = hrd_get_published_qp(master_qp_name.c_str());
    if (master_qp_ == nullptr) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
  XLOGF(INFO, "Client {} found master.", gcid_);

  hrd_connect_qp(cb_, 0, master_qp_);
  XLOGF(INFO, "Client {} connected to master.", gcid_);

  hrd_wait_till_ready(master_qp_name.c_str());
}

void HerdClient::PostRecvWrs(const std::vector<unsigned int>& wr_id_list) {
  XCHECK_LE(static_cast<int>(wr_id_list.size()), WINDOW_SIZE);

  for (unsigned int i = 0; i < wr_id_list.size(); i++) {
    auto wr_id = wr_id_list[i];
    recv_sges_[i].addr =
        reinterpret_cast<uint64_t>(cb_->dgram_buf + wr_id * kDgramEntrySize);

    recv_wrs_[i].wr_id = wr_id;
    recv_wrs_[i].next = &recv_wrs_[i + 1];
    recv_wrs_[i].sg_list = &recv_sges_[i];
  }
  recv_wrs_[wr_id_list.size() - 1].next = nullptr;

  ibv_recv_wr* bad;
  int rc = ibv_post_recv(cb_->dgram_qp[0], recv_wrs_, &bad);
  XLOGF_IF(FATAL, rc != 0, "ibv_post_recv: {} (wr_id={})", strerror(rc),
           bad->wr_id);
}

bool HerdClient::PostRequest(uint128 mica_key, const char* value,
                             unsigned int len, bool update,
                             unsigned int rem_worker) {
  if (pending_req_ == WINDOW_SIZE) {
    // must call GetResponses now
    return false;
  }

  if (pending_unsig_ == UNSIG_BATCH_) {
    ibv_wc wc;
    hrd_poll_cq(cb_->conn_cq[0], 1, &wc);
  }

  mica_op* buf = &req_buf_[req_buf_idx_];
  *(uint128*)buf = mica_key;
  buf->opcode = update ? HERD_OP_PUT : HERD_OP_GET;
  buf->seq = pending_req_;
  buf->val_len = len;
  if (update && value != nullptr) {
    XCHECK_LE(len, sizeof(mica_op::value));
    std::memcpy(buf->value, value, len);
  }

  ibv_sge sge;
  sge.length = update ? HERD_PUT_REQ_SIZE : HERD_GET_REQ_SIZE;
  sge.addr = reinterpret_cast<uint64_t>(buf);
  sge.lkey = req_mr_->lkey;

  ibv_send_wr wr, *bad_wr;
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.next = nullptr;
  wr.send_flags = pending_unsig_ == 0 ? IBV_SEND_SIGNALED : 0;
  if (HERD_VALUE_SIZE <= kInlineCutOff || !update) {
    wr.send_flags |= IBV_SEND_INLINE;
  }

  wr.wr.rdma.remote_addr =
      master_qp_->buf_addr +
      Offset(rem_worker, gcid_, window_slots_[rem_worker]) * sizeof(mica_op);
  wr.wr.rdma.rkey = master_qp_->rkey;

  int ret = ibv_post_send(cb_->conn_qp[0], &wr, &bad_wr);
  XCHECK_EQ(ret, 0) << strerror(ret);

  pending_req_++;
  HRD_MOD_ADD(req_buf_idx_, req_buf_entries_);
  HRD_MOD_ADD(pending_unsig_, UNSIG_BATCH);
  HRD_MOD_ADD(window_slots_[rem_worker], WINDOW_SIZE);
  resps_.emplace_back(mica_key, rem_worker, update);

  return true;
}

void HerdClient::GetResponses(std::vector<HerdResp>& resps) {
  if (pending_req_ == 0) {
    return;
  }

  ibv_wc wc[WINDOW_SIZE];
  hrd_poll_cq(cb_->dgram_recv_cq[0], pending_req_, wc);

  std::vector<unsigned int> recv_wr_ids(pending_req_);

  for (unsigned int i = 0; i < pending_req_; i++) {
    recv_wr_ids[i] = wc[i].wr_id;
    // request sequence number
    uint8_t seq = wc[i].imm_data & 0xffU;
    unsigned int resp_code = wc[i].imm_data >> 8;
    XCHECK(resp_code == HerdResponseCode::kNormal ||
           resp_code == HerdResponseCode::kOffloaded);

    auto& resp = resps_.at(seq);
    if (resp_code == HerdResponseCode::kOffloaded) {
      resp.offloaded_ = true;
    }
    if (!resp.update_) {
      resps_.at(seq).buf_ =
          cb_->dgram_buf + wc[i].wr_id * kDgramEntrySize + sizeof(ibv_grh);
    }
  }

  PostRecvWrs(recv_wr_ids);
  pending_req_ = 0;
  resps.swap(resps_);
  resps_.clear();
}
