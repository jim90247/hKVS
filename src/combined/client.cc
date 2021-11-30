#include <getopt.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/raw_logging.h>

#include <numeric>
#include <thread>
#include <vector>

#include "herd_main.h"
#include "libhrd/hrd.h"
#include "mica/mica.h"

constexpr int DGRAM_BUF_SIZE = 4096;

DEFINE_int32(herd_server_ports, 1, "Number of server ports");
DEFINE_int32(herd_client_ports, 1, "Number of client ports");
// Base port index of HERD: the i-th available IB port (start from 0)
DEFINE_int32(herd_base_port_index, 0, "HERD base port index");
DEFINE_int32(herd_machine_id, 0, "HERD machine id");
DEFINE_int32(threads, 1, "Number of client threads");

DEFINE_int32(update_percentage, 5,
             "Percentage of update/set operations (0~100)");

/** Generate a random permutation of [0, n - 1] for client @clt_gid */
vector<int> get_random_permutation(int n, int clt_gid, uint64_t* seed) {
  /* Each client uses a different range in the cycle space of fastrand */
  for (int i = 0; i < clt_gid * HERD_NUM_KEYS; i++) {
    hrd_fastrand(seed);
  }

  RAW_LOG(INFO, "client %2d: creating a permutation of 0~%d", clt_gid, n - 1);
  vector<int> log(n);
  std::iota(log.begin(), log.end(), 0);

  RAW_DLOG(INFO, "client %d: shuffling..", clt_gid);
  for (int i = n - 1; i >= 1; i--) {
    int j = hrd_fastrand(seed) % (i + 1);
    std::swap(log[i], log[j]);
  }
  RAW_LOG(INFO, "client %2d: done creating random permutation", clt_gid);

  return log;
}

void ClientMain(herd_thread_params herd_params) {
  int i;
  int clt_gid = herd_params.id; /* Global ID of this client thread */
  int num_client_ports = herd_params.num_client_ports;
  int num_server_ports = herd_params.num_server_ports;
  int update_percentage = herd_params.update_percentage;

  /* This is the only port used by this client */
  int ib_port_index = herd_params.base_port_index + clt_gid % num_client_ports;

  /*
   * The virtual server port index to connect to. This index is relative to
   * the server's base_port_index (that the client does not know).
   */
  int srv_virt_port_index = clt_gid % num_server_ports;

  /*
   * TODO: The client creates a connected buffer because the libhrd API
   * requires a buffer when creating connected queue pairs. This should be
   * fixed in the API.
   */
  struct hrd_ctrl_blk* cb = hrd_ctrl_blk_init(
      clt_gid,                /* local_hid */
      ib_port_index, -1,      /* port_index, numa_node_id */
      1, 1,                   /* #conn qps, uc */
      nullptr, 4096, -1,      /* prealloc conn buf, buf size, key */
      1, DGRAM_BUF_SIZE, -1); /* num_dgram_qps, dgram_buf_size, key */

  char mstr_qp_name[HRD_QP_NAME_SIZE];
  sprintf(mstr_qp_name, "master-%d-%d", srv_virt_port_index, clt_gid);

  char clt_conn_qp_name[HRD_QP_NAME_SIZE];
  sprintf(clt_conn_qp_name, "client-conn-%d", clt_gid);
  char clt_dgram_qp_name[HRD_QP_NAME_SIZE];
  sprintf(clt_dgram_qp_name, "client-dgram-%d", clt_gid);

  hrd_publish_conn_qp(cb, 0, clt_conn_qp_name);
  hrd_publish_dgram_qp(cb, 0, clt_dgram_qp_name);
  RAW_LOG(INFO, "Client %s published conn and dgram. Waiting for master %s\n",
          clt_conn_qp_name, mstr_qp_name);

  struct hrd_qp_attr* mstr_qp = nullptr;
  while (mstr_qp == nullptr) {
    mstr_qp = hrd_get_published_qp(mstr_qp_name);
    if (mstr_qp == nullptr) {
      usleep(200000);
    }
  }

  RAW_LOG(INFO, "Client %s found master! Connecting..\n", clt_conn_qp_name);
  hrd_connect_qp(cb, 0, mstr_qp);
  hrd_wait_till_ready(mstr_qp_name);

  /* Start the real work */
  uint64_t seed = 0xdeadbeef;
  vector<int> key_arr = get_random_permutation(HERD_NUM_KEYS, clt_gid, &seed);
  int key_i, ret;

  /* Some tracking info */
  int ws[NUM_WORKERS] = {0}; /* Window slot to use for a worker */

  struct mica_op* req_buf =
      reinterpret_cast<mica_op*>(memalign(4096, sizeof(mica_op)));
  RAW_CHECK(req_buf != nullptr,
            "Failed to allocate 4KB-aligned request buffer");

  struct ibv_send_wr wr, *bad_send_wr;
  struct ibv_sge sgl;
  struct ibv_wc wc[WINDOW_SIZE];

  struct ibv_recv_wr recv_wr[WINDOW_SIZE], *bad_recv_wr;
  struct ibv_sge recv_sgl[WINDOW_SIZE];

  long long rolling_iter = 0; /* For throughput measurement */
  long long nb_tx = 0;        /* Total requests performed or queued */
  int wn = 0;                 /* Worker number */

  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  /* Fill the RECV queue */
  for (i = 0; i < WINDOW_SIZE; i++) {
    hrd_post_dgram_recv(cb->dgram_qp[0], (void*)cb->dgram_buf, DGRAM_BUF_SIZE,
                        cb->dgram_buf_mr->lkey);
  }

  while (1) {
    if (rolling_iter >= K_512) {
      clock_gettime(CLOCK_REALTIME, &end);
      double seconds = (end.tv_sec - start.tv_sec) +
                       (double)(end.tv_nsec - start.tv_nsec) / 1000000000;
      printf("main: Client %2d: %.2f IOPS. nb_tx = %lld\n", clt_gid,
             K_512 / seconds, nb_tx);

      rolling_iter = 0;

      clock_gettime(CLOCK_REALTIME, &start);
    }

    /* Re-fill depleted RECVs */
    if (nb_tx % WINDOW_SIZE == 0 && nb_tx > 0) {
      for (i = 0; i < WINDOW_SIZE; i++) {
        recv_sgl[i].length = DGRAM_BUF_SIZE;
        recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
        recv_sgl[i].addr = (uintptr_t)&cb->dgram_buf[0];

        recv_wr[i].sg_list = &recv_sgl[i];
        recv_wr[i].num_sge = 1;
        recv_wr[i].next = (i == WINDOW_SIZE - 1) ? NULL : &recv_wr[i + 1];
      }

      ret = ibv_post_recv(cb->dgram_qp[0], &recv_wr[0], &bad_recv_wr);
      CPE(ret, "ibv_post_recv error", ret);
    }

    if (nb_tx % WINDOW_SIZE == 0 && nb_tx > 0) {
      hrd_poll_cq(cb->dgram_recv_cq[0], WINDOW_SIZE, wc);
    }

    wn = hrd_fastrand(&seed) % NUM_WORKERS; /* Choose a worker */
    int is_update = (hrd_fastrand(&seed) % 100 < update_percentage) ? 1 : 0;

    /* Forge the HERD request */
    // FIXME: use Zipfian distribution to choose the key
    key_i = hrd_fastrand(&seed) % HERD_NUM_KEYS; /* Choose a key */

    *(uint128*)req_buf = CityHash128_High64((char*)&key_arr[key_i], 4);
    req_buf->opcode = is_update ? HERD_OP_PUT : HERD_OP_GET;
    req_buf->val_len = is_update ? HERD_VALUE_SIZE : -1;

    /* Forge the RDMA work request */
    sgl.length = is_update ? HERD_PUT_REQ_SIZE : HERD_GET_REQ_SIZE;
    sgl.addr = (uint64_t)(uintptr_t)req_buf;

    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.num_sge = 1;
    wr.next = nullptr;
    wr.sg_list = &sgl;

    wr.send_flags = (nb_tx & UNSIG_BATCH_) == 0 ? IBV_SEND_SIGNALED : 0;
    if ((nb_tx & UNSIG_BATCH_) == UNSIG_BATCH_) {
      hrd_poll_cq(cb->conn_cq[0], 1, wc);
    }
    wr.send_flags |= IBV_SEND_INLINE;

    wr.wr.rdma.remote_addr = mstr_qp->buf_addr + Offset(wn, clt_gid, ws[wn]) *
                                                     sizeof(struct mica_op);
    wr.wr.rdma.rkey = mstr_qp->rkey;

    ret = ibv_post_send(cb->conn_qp[0], &wr, &bad_send_wr);
    CPE(ret, "ibv_post_send error", ret);
    // printf("Client %d: sending request index %lld\n", clt_gid, nb_tx);

    rolling_iter++;
    nb_tx++;
    HRD_MOD_ADD(ws[wn], WINDOW_SIZE);
  }
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  /* Use small queues to reduce cache pressure */
  static_assert(HRD_Q_DEPTH == 128);

  /* All requests should fit into the master's request region */
  static_assert(sizeof(mica_op) * NUM_CLIENTS * NUM_WORKERS * WINDOW_SIZE <
                RR_SIZE);

  /* Unsignaled completion checks. worker.c does its own check w/ @postlist */
  static_assert(UNSIG_BATCH >= WINDOW_SIZE); /* Pipelining check for clients */
  static_assert(HRD_Q_DEPTH >= 2 * UNSIG_BATCH); /* Queue capacity check */

  assert(FLAGS_herd_base_port_index >= 0 && FLAGS_herd_base_port_index <= 8);
  assert(FLAGS_herd_server_ports >= 1 && FLAGS_herd_server_ports <= 8);

  assert(FLAGS_herd_client_ports >= 1 && FLAGS_herd_client_ports <= 8);
  CHECK_GE(FLAGS_threads, 1);
  CHECK_GE(FLAGS_herd_machine_id, 0);
  assert(FLAGS_update_percentage >= 0 && FLAGS_update_percentage <= 100);

  /* Launch a single server thread or multiple client threads */
  LOG(INFO) << "Using " << FLAGS_threads << " threads";
  std::vector<std::thread> threads;

  for (int i = 0; i < FLAGS_threads; i++) {
    herd_thread_params param = {
        .id = (FLAGS_herd_machine_id * FLAGS_threads) + i,
        .base_port_index = FLAGS_herd_base_port_index,
        .num_server_ports = FLAGS_herd_server_ports,
        .num_client_ports = FLAGS_herd_client_ports,
        .update_percentage = FLAGS_update_percentage,
        // Does not matter for clients. Client postlist = NUM_WORKERS
        .postlist = -1};
    auto t = std::thread(ClientMain, param);
    threads.emplace_back(std::move(t));
  }

  for (auto& t : threads) {
    t.join();
  }

  return 0;
}
