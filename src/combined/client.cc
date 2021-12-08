#include <getopt.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/raw_logging.h>

#include <numeric>
#include <queue>
#include <thread>
#include <vector>

#include "clover_wrapper/cn.h"
#include "herd_main.h"
#include "libhrd/hrd.h"
#include "mica/mica.h"
#include "util/zipfian_generator.h"

constexpr int DGRAM_BUF_SIZE = 4096;
constexpr int kCloverWorkerCoroutines = 4;
// Clover coroutines + master coroutine
static_assert(kCloverWorkerCoroutines + 1 <= MITSUME_CLT_COROUTINE_NUMBER);

DEFINE_int32(herd_server_ports, 1, "Number of server ports");
DEFINE_int32(herd_client_ports, 1, "Number of client ports");
// Base port index of HERD: the i-th available IB port (start from 0)
DEFINE_int32(herd_base_port_index, 0, "HERD base port index");
DEFINE_int32(herd_machine_id, 0, "HERD machine id");
DEFINE_int32(herd_threads, 1, "Number of threads running HERD client");

DEFINE_int32(clover_threads, 1,
             "Number of threads running Clover compute node");

DEFINE_uint32(update_percentage, 5,
              "Percentage of update/set operations (0~100)");
DEFINE_double(
    zipfian_alpha, 0.99,
    "Zipfian distribution parameter (higher for more skewed distribution)");

/**
 * @brief Generates a key access trace based on Zipfian distribution. Key range:
 * [0, key_range).
 *
 * @param trace_len the length of the trace
 * @param worker_id the worker thread id which will be used as the random seed
 * @param key_range the key range
 * @return a vector of integers in range [0, key_range) representing the
 * trace
 */
vector<int> GenerateZipfianTrace(size_t trace_len, int worker_id,
                                 int key_range) {
  ZipfianGenerator gen(key_range, FLAGS_zipfian_alpha, worker_id);
  vector<int> trace(trace_len);
  size_t offloaded_keys = 0;
  RAW_LOG(INFO, "Start generating Zipfian trace for worker %2d (alpha = %.2f)",
          worker_id, FLAGS_zipfian_alpha);
  for (size_t i = 0; i < trace_len; i++) {
    trace[i] = gen.GetNumber();
    if (trace[i] < kKeysToOffloadPerWorker) {
      offloaded_keys++;
    }
  }
  RAW_LOG(INFO,
          "Done generating Zipfian trace for worker %2d (fraction of offloaded "
          "keys: %.3f)",
          worker_id, static_cast<double>(offloaded_keys) / trace_len);
  return trace;
}

/// HERD thread main function
void HerdMain(herd_thread_params herd_params) {
  int i;
  int clt_gid = herd_params.id; /* Global ID of this client thread */
  int num_client_ports = herd_params.num_client_ports;
  int num_server_ports = herd_params.num_server_ports;
  uint32_t update_percentage = herd_params.update_percentage;

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
  RAW_LOG(INFO, "Client %s published conn and dgram. Waiting for master %s",
          clt_conn_qp_name, mstr_qp_name);

  struct hrd_qp_attr* mstr_qp = nullptr;
  while (mstr_qp == nullptr) {
    mstr_qp = hrd_get_published_qp(mstr_qp_name);
    if (mstr_qp == nullptr) {
      usleep(200000);
    }
  }

  RAW_LOG(INFO, "Client %s found master! Connecting..", clt_conn_qp_name);
  hrd_connect_qp(cb, 0, mstr_qp);
  hrd_wait_till_ready(mstr_qp_name);

  /* Start the real work */
  // the Zipfian trace
  vector<int> trace = GenerateZipfianTrace(16UL << 20, clt_gid, HERD_NUM_KEYS);
  size_t key_i = 0;
  uint64_t seed = 0xdeadbeef;
  int ret;

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

  constexpr long kMaxRollingIter = M_1;
  while (1) {
    if (rolling_iter >= kMaxRollingIter) {
      clock_gettime(CLOCK_REALTIME, &end);
      double sec = (end.tv_sec - start.tv_sec) +
                   (double)(end.tv_nsec - start.tv_nsec) / 1000000000;
      RAW_LOG(INFO, "HERD worker %4d: %12.3f op/s", clt_gid,
              kMaxRollingIter / sec);

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
      RAW_CHECK(ret == 0, strerror(ret));
    }

    if (nb_tx % WINDOW_SIZE == 0 && nb_tx > 0) {
      hrd_poll_cq(cb->dgram_recv_cq[0], WINDOW_SIZE, wc);
    }

    wn = hrd_fastrand(&seed) % NUM_WORKERS; /* Choose a worker */
    int is_update = (hrd_fastrand(&seed) % 100 < update_percentage) ? 1 : 0;

    /* Forge the HERD request */
    int key = trace.at(key_i);
    key_i = key_i < trace.size() - 1 ? key_i + 1 : 0;

    *(uint128*)req_buf = ConvertPlainKeyToHerdKey(key);
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
    RAW_CHECK(ret == 0, strerror(ret));
    // printf("Client %d: sending request index %lld\n", clt_gid, nb_tx);

    rolling_iter++;
    nb_tx++;
    HRD_MOD_ADD(ws[wn], WINDOW_SIZE);
  }
}

void CloverWorkerCoroutine(coro_yield_t& yield,
                           CloverCnThreadWrapper& clover_thread, int coro,
                           std::queue<mitsume_key>& req_queue) {
  char buf[4096] = {0};
  uint32_t len = 0U;

  while (true) {
    if (req_queue.empty()) {
      clover_thread.YieldToAnotherCoro(coro, yield);
      continue;
    }
    mitsume_key key = req_queue.front();
    req_queue.pop();
    RAW_DLOG(INFO, "coro %d read key %lu start", coro, key);
    // will yield to other coroutines which enables concurrent reading
    clover_thread.ReadKVPair(key, buf, &len, 4096, coro, yield);
    RAW_DLOG(INFO, "coro %d read key %lu done", coro, key);
  }
}

/// Main coroutine that dispatch keys in trace to different worker coroutines
/// and report performance numbers.
void CloverMainCoroutine(coro_yield_t& yield,
                         CloverCnThreadWrapper& clover_thread, int thread_id,
                         const std::vector<int>& trace,
                         std::vector<std::queue<mitsume_key>>& req_queues) {
  using clock = std::chrono::steady_clock;
  constexpr size_t kReqDepth = 2;
  size_t idx = 0;

  auto start = clock::now();
  while (true) {
    for (auto& q : req_queues) {
      // Refill the request queue
      while (q.size() < kReqDepth) {
        int raw_key = trace.at(idx++);
        uint128 mica_k = ConvertPlainKeyToHerdKey(raw_key);
        mitsume_key clover_k =
            ConvertHerdKeyToCloverKey(reinterpret_cast<mica_key*>(&mica_k), 0);
        q.push(clover_k);
        if (idx >= trace.size()) {
          RAW_CHECK(idx == trace.size(),
                    "Trace index should never exceed trace size");
          // Report performance each time the full trace is enqueued for
          // processing
          auto end = clock::now();
          double sec = std::chrono::duration<double>(end - start).count();
          RAW_LOG(INFO, "Clover thread %2d: %12.3f op/s", thread_id,
                  trace.size() / sec);

          start = clock::now();
          idx = 0;
        }
      }
    }
    clover_thread.YieldToAnotherCoro(kMasterCoroutineIdx, yield);
  }
}

/// Clover thread main function
void CloverMain(CloverComputeNodeWrapper& clover_node, int clover_thread_id) {
  using std::placeholders::_1;
  // Use local thread id as Clover thread id
  CloverCnThreadWrapper clover_thread(std::ref(clover_node), clover_thread_id);

  RAW_LOG(INFO, "Starting Clover thread %2d", clover_thread_id);

  /// Each clover thread has its own trace. Master coroutine distributes the
  /// trace across worker coroutines.
  std::vector<int> trace = GenerateZipfianTrace(1UL << 20, clover_thread_id,
                                                kKeysToOffloadPerWorker);
  /// The trace is distributed via these request queues (one for each worker).
  std::vector<std::queue<mitsume_key>> req_queues(kCloverWorkerCoroutines);

  clover_thread.RegisterCoroutine(
      coro_call_t(std::bind(CloverMainCoroutine, _1, std::ref(clover_thread),
                            clover_thread_id, std::ref(trace),
                            std::ref(req_queues))),
      kMasterCoroutineIdx);

  for (int coro = 1; coro <= kCloverWorkerCoroutines; coro++) {
    clover_thread.RegisterCoroutine(
        coro_call_t(std::bind(CloverWorkerCoroutine, _1,
                              std::ref(clover_thread), coro,
                              std::ref(req_queues[coro - 1]))),
        coro);
  }

  clover_thread.ExecuteMasterCoroutine();
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  CHECK(FLAGS_herd_base_port_index >= 0 && FLAGS_herd_base_port_index <= 8);
  CHECK(FLAGS_herd_server_ports >= 1 && FLAGS_herd_server_ports <= 8);
  CHECK(FLAGS_herd_client_ports >= 1 && FLAGS_herd_client_ports <= 8);
  // should have at least one client thread of HERD or Clover
  CHECK_GE(FLAGS_herd_threads + FLAGS_clover_threads, 1);
  CHECK_GE(FLAGS_herd_machine_id, 0);
  CHECK_LE(FLAGS_update_percentage, 100);

  LOG(INFO) << "Using " << FLAGS_herd_threads << " threads for HERD and "
            << FLAGS_clover_threads << " threads for Clover";
  LOG(INFO) << "Using " << kCloverWorkerCoroutines
            << " Clover worker coroutines";

  CloverComputeNodeWrapper clover_node(FLAGS_clover_threads);
  /* Since primary KVS (combined_worker) initializes connection to Clover before
   * accepting connections from client, clients should also initializes
   * connection to Clover first before connecting to primary KVS
   * (combined_worker). Otherwise there will be a deadlock, since Clover
   * initialization completes only when all Clover nodes are connected.
   */
  clover_node.Initialize();
  LOG(INFO) << "Done initializing clover compute node";
  LOG(INFO) << "Wait 10 secs for server to populate keys to Clover";
  sleep(10);

  std::vector<std::thread> threads;
  for (int i = 0; i < FLAGS_herd_threads; i++) {
    herd_thread_params param = {
        .id = (FLAGS_herd_machine_id * FLAGS_herd_threads) + i,
        .base_port_index = FLAGS_herd_base_port_index,
        .num_server_ports = FLAGS_herd_server_ports,
        .num_client_ports = FLAGS_herd_client_ports,
        .update_percentage = FLAGS_update_percentage,
        // Does not matter for clients. Client postlist = NUM_WORKERS
        .postlist = -1};
    auto t = std::thread(HerdMain, param);
    threads.emplace_back(std::move(t));
  }
  for (int i = 0; i < FLAGS_clover_threads; i++) {
    auto t = std::thread(std::thread(CloverMain, std::ref(clover_node), i));
    threads.emplace_back(std::move(t));
  }

  for (auto& t : threads) {
    t.join();
  }

  return 0;
}
