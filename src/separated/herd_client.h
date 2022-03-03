#pragma once

#include "combined/herd_main.h"

class HerdResp;

class HerdClient {
 public:
  static inline unsigned int PickRemoteWorker(const uint128 k) {
    return k.second % NUM_WORKERS;
  }
  HerdClient(int gcid, int server_ports, int client_ports, int base_port_index);
  void ConnectToServer();
  bool PostRequest(uint128 mica_key, const char* value, unsigned int len,
                   bool update);
  bool PostRequest(uint128 mica_key, const char* value, unsigned int len,
                   bool update, unsigned int rem_worker);
  void GetResponses(std::vector<HerdResp>& resps);

 private:
  static constexpr int kDgramEntrySize =
      sizeof(ibv_grh) +
      std::max(HERD_VALUE_SIZE, static_cast<int>(sizeof(mitsume_key)));

  // this thread's global client id
  const int gcid_;
  const int num_server_ports_;
  const int num_client_ports_;

  hrd_ctrl_blk* cb_;
  hrd_qp_attr* master_qp_;

  mica_op* req_buf_;
  ibv_mr* req_mr_;
  const unsigned int req_buf_entries_;
  unsigned int req_buf_idx_;
  unsigned int pending_req_;
  unsigned int pending_unsig_;

  std::vector<HerdResp> resps_;

  std::vector<int> window_slots_;

  // work requests for responses
  ibv_recv_wr recv_wrs_[WINDOW_SIZE];
  ibv_sge recv_sges_[WINDOW_SIZE];

  void PostRecvWrs(const std::vector<unsigned int>& wr_id_list);
};

class HerdResp {
  friend bool HerdClient::PostRequest(uint128 mica_key, const char* value,
                                      unsigned int len, bool update,
                                      unsigned int rem_worker);
  friend void HerdClient::GetResponses(std::vector<HerdResp>& resps);

 public:
  HerdResp(uint128 key, unsigned int rem, bool update);
  uint128 Key() const;
  mitsume_key CloverKey() const;
  const volatile uint8_t* ValuePtr() const;
  bool Offloaded() const;

 private:
  uint128 key_;
  const volatile uint8_t* buf_;
  const unsigned int rem_;
  bool offloaded_;
  bool update_;
};
