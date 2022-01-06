#include <getopt.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/raw_logging.h>

#include <numeric>
#include <queue>
#include <set>
#include <thread>
#include <unordered_set>
#include <vector>

#include "clover_worker.h"
#include "clover_wrapper/cn.h"
#include "herd_main.h"
#include "libhrd/hrd.h"
#include "mica/mica.h"
#include "util/zipfian_generator.h"

using CoroRequestQueue = std::queue<mitsume_key>;

// the size of each data gram buffer element
constexpr uint32_t kDgramEntrySize =
    sizeof(ibv_grh) +
    std::max(HERD_VALUE_SIZE, static_cast<int>(sizeof(mitsume_key)));
constexpr int DGRAM_BUF_SIZE = std::max(4096U, kDgramEntrySize* WINDOW_SIZE);

DEFINE_int32(herd_server_ports, 1, "Number of server ports");
DEFINE_int32(herd_client_ports, 1, "Number of client ports");
// Base port index of HERD: the i-th available IB port (start from 0)
DEFINE_int32(herd_base_port_index, 0, "HERD base port index");
DEFINE_int32(herd_machine_id, 0, "HERD machine id");
DEFINE_int32(herd_threads, 1, "Number of threads running HERD client");

DEFINE_uint32(clover_batch, 32, "Clover request submit batch size");
DEFINE_uint32(clover_max_cncr, 4,
              "Maximum concurrent batches of Clover requests");

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

enum class CloverState {
  kPreparing,     // enqueuing new requests to buffer
  kWaitForResp,   // waiting for responses from previous batches
  kReadyToSubmit  // request buffer is full and we're allowed to submit them
};

/**
 * @brief Posts receive work requests and aborts on failure.
 *
 * @param qp the queue pair
 * @param wr pointer to the list of work requests
 */
void PostRecvWrs(ibv_qp* const qp, ibv_recv_wr* const wr) {
  ibv_recv_wr* bad;
  int rc = ibv_post_recv(qp, wr, &bad);
  LOG_IF(FATAL, rc) << "ibv_post_recv: " << strerror(rc)
                    << " (wr_id=" << bad->wr_id << ")";
}

/// HERD thread main function
void HerdMain(herd_thread_params herd_params, int local_id,
              SharedRequestQueue& req_queue,
              std::shared_ptr<SharedResponseQueue> resp_queue_ptr) {
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

  // Request queue producer token for this worker thread
  moodycamel::ProducerToken ptok(req_queue);
  // Clover request buffer
  std::vector<CloverRequest> clover_req_buf(FLAGS_clover_batch);
  // index of Clover request buffer
  unsigned int clover_req_idx = 0;
  // Clover response buffer
  CloverResponse* clover_resp_buf =
      new CloverResponse[FLAGS_clover_batch * FLAGS_clover_max_cncr];
  // Stores which keys exist in Clover
  std::unordered_set<mitsume_key> lookup_table;
  constexpr unsigned int kCloverReadBufEntrySize = 4096;
  // Clover batch id (modulo FLAGS_clover_max_cncr)
  unsigned int clover_bid = 0;
  // Clover read buffer
  char* clover_read_buf = new char[FLAGS_clover_max_cncr * FLAGS_clover_batch *
                                   kCloverReadBufEntrySize]();
  // Current Clover state
  CloverState clover_state = CloverState::kPreparing;
  // Free Clover batch ids. Used for limiting maximum concurrency.
  std::set<unsigned int> avail_batch_ids;
  for (unsigned int i = 0; i < FLAGS_clover_max_cncr; i++) {
    avail_batch_ids.insert(i);
  }

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
  mitsume_key req_keys[WINDOW_SIZE];

  struct ibv_recv_wr recv_wr[WINDOW_SIZE];
  struct ibv_sge recv_sgl[WINDOW_SIZE];

  for (int i = 0; i < WINDOW_SIZE; i++) {
    recv_sgl[i].addr =
        reinterpret_cast<uintptr_t>(cb->dgram_buf + i * kDgramEntrySize);
    recv_sgl[i].length = kDgramEntrySize;
    recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;

    recv_wr[i].wr_id = i;
    recv_wr[i].next = (i == WINDOW_SIZE - 1) ? nullptr : &recv_wr[i + 1];
    recv_wr[i].sg_list = recv_sgl + i;
    recv_wr[i].num_sge = 1;
  }

  long long rolling_iter = 0; /* For throughput measurement */
  long long nb_tx = 0;        /* Total requests performed or queued */
  int wn = 0;                 /* Worker number */

  // Completed Clover batches (reset periodically) (for perf measurement)
  long clover_comp_batch = 0;
  // Failed Clover requests (reset periodically) (for perf measurement)
  long clover_fails = 0;

  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  /* Fill the RECV queue */
  PostRecvWrs(cb->dgram_qp[0], recv_wr);

  constexpr long kMaxRollingIter = M_1;
  while (1) {
    if (rolling_iter >= kMaxRollingIter) {
      clock_gettime(CLOCK_REALTIME, &end);
      double sec = (end.tv_sec - start.tv_sec) +
                   (double)(end.tv_nsec - start.tv_nsec) / 1000000000;
      LOG(INFO) << "Worker " << clt_gid << ", HERD: " << rolling_iter / sec
                << "/s, Clover completed: "
                << clover_comp_batch * FLAGS_clover_batch / sec
                << "/s, failures: " << clover_fails / sec << "/s";
      LOG_IF(INFO, local_id == 0)
          << "Approx. queued Clover requests: " << req_queue.size_approx();

      rolling_iter = 0;
      clover_comp_batch = 0;
      clover_fails = 0;

      clock_gettime(CLOCK_REALTIME, &start);
    }

    if (nb_tx % WINDOW_SIZE == 0 && nb_tx > 0) {
      hrd_poll_cq(cb->dgram_recv_cq[0], WINDOW_SIZE, wc);

      for (int w = 0; w < WINDOW_SIZE; w++) {
        uint8_t seq = wc[w].imm_data & 0xffU;
        unsigned int resp_code = wc[w].imm_data >> 8;
        DCHECK(resp_code == HerdResponseCode::kNormal ||
               resp_code == HerdResponseCode::kOffloaded);

        if (resp_code == HerdResponseCode::kOffloaded) {
          lookup_table.insert(req_keys[seq]);
          DLOG(INFO) << "New key in Clover " << std::hex << req_keys[seq]
                     << std::dec;
        }
      }

      /* Re-fill depleted RECVs */
      PostRecvWrs(cb->dgram_qp[0], recv_wr);
    }

    wn = hrd_fastrand(&seed) % NUM_WORKERS; /* Choose a worker */
    int is_update = (hrd_fastrand(&seed) % 100 < update_percentage) ? 1 : 0;

    /* Forge the HERD request */
    int key = trace.at(key_i);
    key_i = key_i < trace.size() - 1 ? key_i + 1 : 0;

    if (clover_state == CloverState::kPreparing && !is_update &&
        FLAGS_clover_threads > 0) {
      uint128 mica_k = ConvertPlainKeyToHerdKey(key);
      mitsume_key clover_k =
          ConvertHerdKeyToCloverKey(reinterpret_cast<mica_key*>(&mica_k), wn);
      if (lookup_table.find(clover_k) != lookup_table.end()) {
        /* Process current request using Clover */
        unsigned int req_id = clover_bid * FLAGS_clover_batch + clover_req_idx;
        char* rbuf = clover_read_buf + req_id * kCloverReadBufEntrySize;
        // To reduce the amount of responses, requires reply on last request of
        // each batch. For other requests, ask for reply only when they failed
        // (used for updating lookup table).
        CloverReplyOption reply_opt = clover_req_idx == FLAGS_clover_batch - 1
                                          ? CloverReplyOption::kAlways
                                          : CloverReplyOption::kOnFailure;
        clover_req_buf.at(clover_req_idx) = CloverRequest{
            clover_k,                  // key
            rbuf,                      // buf
            kCloverReadBufEntrySize,   // len
            req_id,                    // id
            CloverRequestType::kRead,  // op
            local_id,                  // from
            reply_opt                  // reply_opt
        };
        clover_req_idx++;

        if (clover_req_idx == FLAGS_clover_batch) {
          // wait for previous batches to complete if we reach concurrency limit
          clover_state =
              avail_batch_ids.find(clover_bid) == avail_batch_ids.end()
                  ? CloverState::kWaitForResp
                  : CloverState::kReadyToSubmit;
        }

        // NOTE: use "continue" here will cause blocking at hrd_poll_cq() above
        // workaround: build next HERD request and send it
        is_update = (hrd_fastrand(&seed) % 100 < update_percentage) ? 1 : 0;
        key = trace.at(key_i);
        key_i = key_i < trace.size() - 1 ? key_i + 1 : 0;
      }
    }

    if (clover_state == CloverState::kWaitForResp) {
      int comps = resp_queue_ptr->try_dequeue_bulk(
          clover_resp_buf, FLAGS_clover_batch * FLAGS_clover_max_cncr);

      for (int j = 0; j < comps; j++) {
        const CloverResponse& resp = clover_resp_buf[j];
        if (resp.id % FLAGS_clover_batch == FLAGS_clover_batch - 1) {
          clover_comp_batch++;
          // batch id is (request id / batch size)
          avail_batch_ids.insert(clover_resp_buf[j].id / FLAGS_clover_batch);
        }
        if (resp.rc != MITSUME_SUCCESS) {
          clover_fails++;
          lookup_table.erase(resp.key);
        }
      }
      if (avail_batch_ids.find(clover_bid) != avail_batch_ids.end()) {
        clover_state = CloverState::kReadyToSubmit;
      }
    }

    if (clover_state == CloverState::kReadyToSubmit) {
      DCHECK(avail_batch_ids.find(clover_bid) != avail_batch_ids.end());
      while (!req_queue.try_enqueue_bulk(ptok, clover_req_buf.begin(),
                                         FLAGS_clover_batch))
        ;
      clover_req_idx = 0;
      avail_batch_ids.erase(clover_bid);
      clover_bid = (clover_bid + 1) % FLAGS_clover_max_cncr;
      clover_state = CloverState::kPreparing;
    }

    *(uint128*)req_buf = ConvertPlainKeyToHerdKey(key);
    req_buf->opcode = is_update ? HERD_OP_PUT : HERD_OP_GET;
    req_buf->seq = static_cast<uint8_t>(nb_tx % WINDOW_SIZE);
    req_buf->val_len = is_update ? HERD_VALUE_SIZE : -1;

    req_keys[nb_tx % WINDOW_SIZE] =
        ConvertHerdKeyToCloverKey(&req_buf->key, wn);

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

int main(int argc, char* argv[]) {
  FLAGS_colorlogtostderr = true;
  FLAGS_logtostderr = true;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();

  CHECK(FLAGS_herd_base_port_index >= 0 && FLAGS_herd_base_port_index <= 8);
  CHECK(FLAGS_herd_server_ports >= 1 && FLAGS_herd_server_ports <= 8);
  CHECK(FLAGS_herd_client_ports >= 1 && FLAGS_herd_client_ports <= 8);
  // should have at least one client thread of HERD or Clover
  CHECK_GE(FLAGS_herd_threads + FLAGS_clover_threads, 1);
  CHECK_GE(FLAGS_herd_machine_id, 0);
  CHECK_LE(FLAGS_update_percentage, 100);

  // Clover coroutines + master coroutine
  CHECK_LE(FLAGS_clover_coros + 1, MITSUME_CLT_COROUTINE_NUMBER);

  LOG(INFO) << "Using " << FLAGS_herd_threads << " threads for HERD and "
            << FLAGS_clover_threads << " threads for Clover";
  LOG(INFO) << "Using " << FLAGS_clover_coros << " Clover worker coroutines";
  LOG(INFO) << "Clover request submit batch size: " << FLAGS_clover_batch
            << ", max concurrent batches: " << FLAGS_clover_max_cncr;

  CloverComputeNodeWrapper clover_node(FLAGS_clover_threads);
  /* Since primary KVS (combined_worker) initializes connection to Clover before
   * accepting connections from client, clients should also initializes
   * connection to Clover first before connecting to primary KVS
   * (combined_worker). Otherwise there will be a deadlock, since Clover
   * initialization completes only when all Clover nodes are connected.
   */
  clover_node.Initialize();
  LOG(INFO) << "Done initializing clover compute node";
  // Mitigate a bug in Clover that causes the assertions in
  // userspace_wait_wr_table_value to fail
  LOG(INFO) << "Sleep 3 secs to wait Clover metadata server complete setup";
  sleep(3);

  SharedRequestQueue clover_req_queue(
      FLAGS_clover_max_cncr * FLAGS_clover_batch, FLAGS_herd_threads, 0);
  std::vector<std::shared_ptr<SharedResponseQueue>> clover_resp_queues;
  // using clover_resp_queues(NUM_WORKERS,
  // std::make_shared<SharedResponseQueue>(...)) will create multiple pointers
  // pointing to the same queue
  for (int i = 0; i < FLAGS_herd_threads; i++) {
    clover_resp_queues.emplace_back(std::make_shared<SharedResponseQueue>(
        FLAGS_clover_max_cncr * FLAGS_clover_batch, 0, FLAGS_clover_threads));
  }

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
    auto t = std::thread(HerdMain, param, i, std::ref(clover_req_queue),
                         clover_resp_queues.at(i));
    threads.emplace_back(std::move(t));
  }
  for (int i = 0; i < FLAGS_clover_threads; i++) {
    auto t = std::thread(CloverThreadMain, std::ref(clover_node),
                         std::ref(clover_req_queue),
                         std::cref(clover_resp_queues), i);
    threads.emplace_back(std::move(t));
  }

  for (auto& t : threads) {
    t.join();
  }

  return 0;
}
